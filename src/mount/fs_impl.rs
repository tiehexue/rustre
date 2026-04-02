//! `fuser::Filesystem` trait implementation for `RustreFs`.
//!
//! Every FUSE callback is implemented here. Key design decisions:
//!
//! 1. **Immediate commit on create()**: files are committed to MDS immediately
//!    (size=0) so they are visible to lookups/readdir right away. Writes go
//!    through write_through() directly to OST; final size is synced on
//!    flush/release via SetSizeByIno.
//!
//! 2. **Open-unlink (detached) semantics**: when a file is unlinked while it
//!    has open fh's, the MDS record is removed immediately but OSS object
//!    cleanup is deferred to the last release().
//!
//! 3. **Per-inode flush**: flush/fsync/release flush ALL open handles for the
//!    same inode, ensuring cross-fd consistency (required by git/cargo).
//!
//! 4. **Opendir snapshot**: directory entries are snapshot at opendir time so
//!    concurrent deletes don't shift the readdir offset list.
//!
//! 5. **Write-through for aligned writes**: whole-chunk writes go directly to
//!    OST; partial-chunk writes are buffered and merged on flush.
//!
//! 6. **mknod fallback**: implements mknod for platforms that don't call
//!    create() for O_CREAT.

use super::helpers::{mds_error_to_errno, meta_to_attr, rustre_err_to_errno};
use super::types::*;

use crate::rpc::{rpc_call, RpcKind};
use crate::types::{CreateReq, FileMeta};

use fuser::{
    AccessFlags, BsdFileFlags, CopyFileRangeFlags, Errno, FileHandle, FopenFlags, Generation,
    INodeNo, InitFlags, IoctlFlags, LockOwner, OpenFlags, RenameFlags, TimeOrNow, WriteFlags,
};
use fuser::{
    FileType, Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request,
};

use std::ffi::OsStr;
use std::sync::atomic::Ordering;
use std::time::SystemTime;
use tracing::{debug, error, info};

impl Filesystem for RustreFs {
    // -----------------------------------------------------------------------
    // init
    // -----------------------------------------------------------------------
    fn init(
        &mut self,
        _req: &Request,
        config: &mut fuser::KernelConfig,
    ) -> Result<(), std::io::Error> {
        let _ = config.add_capabilities(InitFlags::FUSE_DIRECT_IO_ALLOW_MMAP);
        info!("RustreFs: FUSE filesystem initialized");
        Ok(())
    }

    // -----------------------------------------------------------------------
    // access
    // -----------------------------------------------------------------------
    fn access(&self, _req: &Request, ino: INodeNo, _mask: AccessFlags, reply: ReplyEmpty) {
        if self.inodes.get_meta(ino.0).is_some() {
            reply.ok();
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    // -----------------------------------------------------------------------
    // lookup
    // -----------------------------------------------------------------------
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let path = match self.child_path(parent.0, name) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };

        match self.stat_path(&path) {
            Ok(meta) => {
                let attr = meta_to_attr(&meta);
                self.inodes.insert(meta);
                reply.entry(&ENTRY_TTL, &attr, Generation(0));
            }
            Err(e) => reply.error(e),
        }
    }

    // -----------------------------------------------------------------------
    // getattr
    // -----------------------------------------------------------------------
    fn getattr(&self, _req: &Request, ino: INodeNo, fh: Option<FileHandle>, reply: ReplyAttr) {
        let ino_val = ino.0;

        // Collect the best (largest) known size from all sources
        let mut best_size: Option<u64> = None;
        let mut base_meta: Option<FileMeta> = None;

        // Source 1: inode cache (updated by flush_writes)
        if let Some(cached) = self.inodes.get_meta(ino_val) {
            best_size = Some(cached.size);
            base_meta = Some(cached);
        }

        // Source 2: per-inode tracked size (updated by write callbacks)
        if let Some(state) = self.inode_state.get(&ino_val) {
            let tracked = state.tracked_size.load(Ordering::Relaxed);
            best_size = Some(best_size.map_or(tracked, |s: u64| s.max(tracked)));
        }

        // Source 3: open file handle snapshot
        if let Some(fh_val) = fh {
            if let Some(file_ref) = self.open_files.get(&fh_val.0) {
                let file = file_ref.value();
                if base_meta.is_none() {
                    base_meta = Some(file.meta.clone());
                }
                best_size = Some(best_size.map_or(file.meta.size, |s: u64| s.max(file.meta.size)));
            }
        }

        if let Some(mut meta) = base_meta {
            if let Some(sz) = best_size {
                meta.size = sz;
            }
            // Use zero TTL for inodes that have active writers, so the kernel
            // always re-checks the size before mmap page faults. This is
            // critical for git's index-pack which reads a pack file via mmap
            // while it's still being written.
            let has_writers = self
                .inode_state
                .get(&ino_val)
                .map(|s| *s.size_dirty.lock())
                .unwrap_or(false);
            let ttl = if has_writers {
                std::time::Duration::ZERO
            } else {
                ATTR_TTL
            };
            return reply.attr(&ttl, &meta_to_attr(&meta));
        }

        reply.error(Errno::ENOENT);
    }

    // -----------------------------------------------------------------------
    // setattr
    // -----------------------------------------------------------------------
    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let ino_val = ino.0;

        // Handle permission changes
        if mode.is_some() || uid.is_some() || gid.is_some() {
            let path = match self.inodes.get_path(ino_val) {
                Some(p) => p,
                None => return reply.error(Errno::ENOENT),
            };

            let mds = match self.pick_mds() {
                Ok(m) => m,
                Err(e) => return reply.error(e),
            };

            let result = self.rt.block_on(async {
                let rpc_reply = rpc_call(
                    &mds,
                    RpcKind::SetPerms {
                        path: path.clone(),
                        mode,
                        uid,
                        gid,
                    },
                )
                .await
                .map_err(rustre_err_to_errno)?;
                match rpc_reply.kind {
                    RpcKind::Ok => Ok(()),
                    RpcKind::Error(e) => Err(mds_error_to_errno(&e)),
                    _ => Err(Errno::EIO),
                }
            });

            if let Err(e) = result {
                return reply.error(e);
            }

            // Update cache
            if let Some(mut meta) = self.inodes.entries.get_mut(&ino_val) {
                if let Some(mode_val) = mode {
                    meta.mode = mode_val;
                }
                if let Some(uid_val) = uid {
                    meta.uid = uid_val;
                }
                if let Some(gid_val) = gid {
                    meta.gid = gid_val;
                }
                meta.mtime = FileMeta::now_secs();
            }
        }

        // Handle truncate
        if let Some(new_size) = size {
            // Update tracked size in per-inode state
            if let Some(state) = self.inode_state.get(&ino_val) {
                state.tracked_size.store(new_size, Ordering::Relaxed);
            }
            self.inodes.update_size(ino_val, new_size);

            // Sync to MDS
            let path = match self.inodes.get_path(ino_val) {
                Some(p) => p,
                None => return reply.error(Errno::ENOENT),
            };

            if let Err(e) = self.sync_size_to_mds(ino_val, &path, new_size) {
                return reply.error(e);
            }

            // Update open files too
            if let Some(fh_val) = fh {
                if let Some(mut file_ref) = self.open_files.get_mut(&fh_val.0) {
                    file_ref.meta.size = new_size;
                }
            }
        }

        // Return current attributes
        match self.inodes.get_meta(ino_val) {
            Some(mut meta) => {
                if let Some(state) = self.inode_state.get(&ino_val) {
                    let tracked = state.tracked_size.load(Ordering::Relaxed);
                    if tracked > meta.size {
                        meta.size = tracked;
                    }
                }
                reply.attr(&ATTR_TTL, &meta_to_attr(&meta));
            }
            None => reply.error(Errno::ENOENT),
        }
    }

    // -----------------------------------------------------------------------
    // mkdir
    // -----------------------------------------------------------------------
    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        let path = match self.child_path(parent.0, name) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };

        let mds = match self.pick_mds() {
            Ok(m) => m,
            Err(e) => return reply.error(e),
        };

        let effective_mode = mode & !umask;
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };

        let result = self.rt.block_on(async {
            let rpc_reply = rpc_call(
                &mds,
                RpcKind::MkdirWithPerms {
                    path: path.clone(),
                    mode: effective_mode,
                    uid,
                    gid,
                },
            )
            .await
            .map_err(rustre_err_to_errno)?;
            match rpc_reply.kind {
                RpcKind::Ok => Ok(()),
                RpcKind::Error(e) => Err(mds_error_to_errno(&e)),
                _ => Err(Errno::EIO),
            }
        });

        if let Err(e) = result {
            return reply.error(e);
        }

        match self.stat_path(&path) {
            Ok(meta) => {
                let attr = meta_to_attr(&meta);
                self.inodes.insert(meta);
                reply.entry(&ENTRY_TTL, &attr, Generation(0));
            }
            Err(e) => reply.error(e),
        }
    }

    // -----------------------------------------------------------------------
    // mknod — regular file creation fallback
    // -----------------------------------------------------------------------
    fn mknod(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        // Only handle regular files
        let file_type = mode & libc::S_IFMT as u32;
        if file_type != libc::S_IFREG as u32 && file_type != 0 {
            return reply.error(Errno::ENOSYS);
        }

        let path = match self.child_path(parent.0, name) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };

        let mds = match self.pick_mds() {
            Ok(m) => m,
            Err(e) => return reply.error(e),
        };

        let effective_mode = (mode & 0o7777) & !umask;
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };

        let result = self.rt.block_on(async {
            let rpc_reply = rpc_call(
                &mds,
                RpcKind::Mknod {
                    path: path.clone(),
                    mode: effective_mode,
                    uid,
                    gid,
                },
            )
            .await
            .map_err(rustre_err_to_errno)?;
            match rpc_reply.kind {
                RpcKind::MetaReply(m) => Ok(m),
                RpcKind::Error(e) => Err(mds_error_to_errno(&e)),
                _ => Err(Errno::EIO),
            }
        });

        match result {
            Ok(meta) => {
                let attr = meta_to_attr(&meta);
                self.inodes.insert(meta);
                reply.entry(&ENTRY_TTL, &attr, Generation(0));
            }
            Err(e) => reply.error(e),
        }
    }

    // -----------------------------------------------------------------------
    // create — create + open (atomic)
    // -----------------------------------------------------------------------
    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let path = match self.child_path(parent.0, name) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };

        let mds = match self.pick_mds() {
            Ok(m) => m,
            Err(e) => return reply.error(e),
        };

        let effective_mode = mode & !umask;
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };

        // Phase 1: Create pending file on MDS
        let meta = self.rt.block_on(async {
            let rpc_reply = rpc_call(
                &mds,
                RpcKind::Create(CreateReq {
                    path: path.clone(),
                    stripe_count: DEFAULT_STRIPE_COUNT,
                    stripe_size: DEFAULT_STRIPE_SIZE,
                    replica_count: 1,
                    mode: effective_mode,
                    uid,
                    gid,
                }),
            )
            .await
            .map_err(rustre_err_to_errno)?;
            match rpc_reply.kind {
                RpcKind::MetaReply(m) => Ok(m),
                RpcKind::Error(e) => Err(mds_error_to_errno(&e)),
                _ => Err(Errno::EIO),
            }
        });

        let meta = match meta {
            Ok(m) => m,
            Err(e) => return reply.error(e),
        };

        let ino = meta.ino;

        // Phase 2: Immediately commit so the file is visible to lookups
        let commit_result = self.rt.block_on(async {
            let mds_addr = self.pick_mds()?;
            let reply = rpc_call(&mds_addr, RpcKind::CommitCreate { ino, size: 0 })
                .await
                .map_err(rustre_err_to_errno)?;
            match reply.kind {
                RpcKind::Ok => Ok(()),
                RpcKind::Error(e) => {
                    error!("immediate commit after create failed: {e}");
                    Err(Errno::EIO)
                }
                _ => Err(Errno::EIO),
            }
        });

        if let Err(e) = commit_result {
            // Best-effort abort
            self.rt.block_on(async {
                if let Ok(addr) = self.pick_mds() {
                    let _ = rpc_call(&addr, RpcKind::AbortCreate { ino }).await;
                }
            });
            return reply.error(e);
        }

        let mut meta = meta;
        meta.pending = false;

        let attr = meta_to_attr(&meta);
        let fh = self.alloc_fh();

        // Cache the inode
        self.inodes.insert(meta.clone());

        // Create per-inode state
        let inode_st = self.get_or_create_inode_state(ino, 0);
        inode_st.acquire();

        // Create open file handle
        let open_file = OpenFile {
            ino,
            meta,
            write_buf: parking_lot::Mutex::new(Vec::new()),
            writable: true,
        };
        self.open_files.insert(fh, open_file);

        debug!("create: ino={ino:#x} fh={fh} path={}", attr.ino);

        reply.created(
            &ENTRY_TTL,
            &attr,
            Generation(0),
            FileHandle(fh),
            FopenFlags::empty(),
        );
    }

    // -----------------------------------------------------------------------
    // open
    // -----------------------------------------------------------------------
    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let ino_val = ino.0;
        let mut meta = match self.inodes.get_meta(ino_val) {
            Some(m) => m,
            None => return reply.error(Errno::ENOENT),
        };

        let raw_flags = flags.0;
        let writable = (raw_flags & libc::O_WRONLY != 0) || (raw_flags & libc::O_RDWR != 0);
        let truncate = raw_flags & libc::O_TRUNC != 0;

        // CRITICAL for mmap correctness: if we're opening a read-only handle
        // and there are active writers, flush them NOW so the MDS has the
        // correct final size. The kernel will getattr right after open() and
        // use the reported size to set up the mmap region. If the size is
        // stale, mmap reads will be short and git will get "nonexistent object".
        if !writable {
            self.flush_all_handles_for_ino(ino_val);
            // Re-fetch meta from inode cache (may have been updated by flush)
            meta = match self.inodes.get_meta(ino_val) {
                Some(m) => m,
                None => return reply.error(Errno::ENOENT),
            };
        }

        // Handle O_TRUNC
        if truncate && writable && meta.size > 0 {
            if let Err(e) = self.sync_size_to_mds(ino_val, &meta.path, 0) {
                return reply.error(e);
            }
            meta.size = 0;
            self.inodes.update_size(ino_val, 0);
        }

        let fh = self.alloc_fh();

        // Get or create per-inode state
        let inode_st = self.get_or_create_inode_state(ino_val, meta.size);
        inode_st.acquire();

        if truncate && writable {
            inode_st.tracked_size.store(0, Ordering::Relaxed);
        }

        debug!(
            "open: ino={ino_val:#x} fh={fh} flags={raw_flags:#x} writable={writable} \
             truncate={truncate} meta.size={} path={}",
            meta.size, meta.path,
        );

        let open_file = OpenFile {
            ino: ino_val,
            meta,
            write_buf: parking_lot::Mutex::new(Vec::new()),
            writable,
        };

        self.open_files.insert(fh, open_file);

        // Do NOT use FOPEN_DIRECT_IO on any handle.
        //
        // On macOS, git reads pack files via mmap which uses the kernel page
        // cache.  If writable handles use DIRECT_IO, writes bypass the page
        // cache, so when a read handle later mmaps the file the kernel serves
        // stale/empty pages → "nonexistent object" errors.
        //
        // Without DIRECT_IO, the kernel page cache is consistent: writes go
        // through the cache, and mmap reads see the latest data.  macFUSE's
        // overlapping-write replays are handled correctly by write_through()
        // which does read-merge-write on the OST chunk.
        reply.opened(FileHandle(fh), FopenFlags::empty());
    }

    // -----------------------------------------------------------------------
    // read
    // -----------------------------------------------------------------------
    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let ino_val = ino.0;

        // Flush dirty handles for read-after-write consistency, but only
        // if this inode actually has outstanding writes (cheap check first).
        // IMPORTANT: drop the DashMap Ref before calling flush to avoid
        // holding shard locks across block_on().
        let needs_flush = self
            .inode_state
            .get(&ino_val)
            .map(|s| *s.size_dirty.lock())
            .unwrap_or(false);
        if needs_flush {
            self.flush_all_handles_for_ino(ino_val);
        }

        // Get metadata with the MOST UP-TO-DATE size.
        // The file may have grown since this handle was opened (e.g., git opens
        // the pack file for reading while still writing to it). We must use the
        // maximum size from all sources to avoid truncated reads.
        let meta = if let Some(file_ref) = self.open_files.get(&fh.0) {
            let mut m = file_ref.meta.clone();
            let mut best_size = m.size;
            // Check per-inode tracked size (updated by concurrent writes)
            if let Some(state) = self.inode_state.get(&ino_val) {
                let tracked = state.tracked_size.load(Ordering::Relaxed);
                if tracked > best_size {
                    best_size = tracked;
                }
            }
            // Check the inode cache (updated by flush_writes → sync_size_to_mds)
            if let Some(cached) = self.inodes.get_meta(ino_val) {
                if cached.size > best_size {
                    best_size = cached.size;
                }
            }
            m.size = best_size;
            m
        } else if let Some(m) = self.inodes.get_meta(ino_val) {
            m
        } else {
            return reply.error(Errno::ENOENT);
        };

        if meta.is_dir {
            return reply.error(Errno::EISDIR);
        }
        if meta.symlink_target.is_some() {
            return reply.error(Errno::EINVAL);
        }

        match self.read_data(&meta, offset, size) {
            Ok(data) => reply.data(&data),
            Err(e) => reply.error(e),
        }
    }

    // -----------------------------------------------------------------------
    // write — write-through for aligned, buffer for partial
    // -----------------------------------------------------------------------
    fn write(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        let fh_val = fh.0;
        let (ino, meta, current_size) = match self.open_files.get(&fh_val) {
            Some(file_ref) => {
                let file = file_ref.value();
                if !file.writable {
                    return reply.error(Errno::EBADF);
                }
                if file.meta.symlink_target.is_some() {
                    return reply.error(Errno::EINVAL);
                }
                let tracked = self
                    .inode_state
                    .get(&file.ino)
                    .map(|s| s.tracked_size.load(Ordering::Relaxed))
                    .unwrap_or(file.meta.size);
                (
                    file.ino,
                    file.meta.clone(),
                    std::cmp::max(file.meta.size, tracked),
                )
            }
            None => return reply.error(Errno::EBADF),
        };

        let new_end = offset + data.len() as u64;
        let logical_size = std::cmp::max(current_size, new_end);

        let layout = match meta.layout.as_ref() {
            Some(l) => l,
            None => return reply.error(Errno::EIO),
        };

        let chunk_size = layout.stripe_size;
        let _write_end = offset + data.len() as u64;

        // Check if write covers at least one FULL chunk (aligned start + chunk-length)
        let starts_on_boundary = offset.is_multiple_of(chunk_size);
        let mut covers_full_chunk = false;
        if starts_on_boundary && data.len() as u64 >= chunk_size {
            covers_full_chunk = true;
        }

        // Full-chunk writes can go directly to OST (no read-merge needed).
        // Partial-chunk writes MUST be buffered and flushed as a batch to
        // avoid the read-modify-write race: macFUSE sends overlapping writes
        // at the same offset with increasing sizes. If we write_through each
        // one, concurrent read-merge-write cycles corrupt the chunk (161
        // overwrites observed for a single 214KB pack file).
        //
        // The macOS kernel page cache keeps the latest data for the write
        // handle's inode, so mmap reads (git pack) see correct data even
        // without write_through. The OST is updated on flush/release.
        if covers_full_chunk {
            if let Err(e) = self.write_through(ino, &meta, logical_size, offset, data) {
                return reply.error(e);
            }
            // CRITICAL: a full-chunk write_through to OST supersedes any
            // previously buffered partial writes that fall within the same
            // byte range.  If we don't drain them, flush_writes will later
            // read-merge-write stale partial data on top of the (correct)
            // write_through data, corrupting the file.
            //
            // This is the root cause of .rmeta corruption: macFUSE sends
            // incremental partial writes (buffered), then a final full-chunk
            // replay (write_through). Without draining, flush merges the
            // old partials back over the correct data.
            let wt_end = offset + data.len() as u64;
            if let Some(file_ref) = self.open_files.get_mut(&fh_val) {
                let mut wb = file_ref.write_buf.lock();
                wb.retain(|(buf_off, buf_data)| {
                    let buf_end = *buf_off + buf_data.len() as u64;
                    // Keep entries that are entirely outside the write_through range
                    buf_end <= offset || *buf_off >= wt_end
                });
            }
        } else {
            if let Some(file_ref) = self.open_files.get_mut(&fh_val) {
                let mut wb = file_ref.write_buf.lock();
                wb.push((offset, data.to_vec()));
            }
        }

        // Update per-inode tracked size
        if let Some(state) = self.inode_state.get(&ino) {
            state.update_size(logical_size);
            *state.size_dirty.lock() = true;
        }
        self.inodes.update_size(ino, logical_size);

        reply.written(data.len() as u32);
    }

    // -----------------------------------------------------------------------
    // flush — called on every close() of an fd
    // -----------------------------------------------------------------------
    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        debug!("flush: fh={}", fh.0);
        match self.flush_writes(fh.0) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    // -----------------------------------------------------------------------
    // fsync
    // -----------------------------------------------------------------------
    fn fsync(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        // Flush all handles for this inode for full consistency
        self.flush_all_handles_for_ino(ino.0);
        reply.ok();
    }

    // -----------------------------------------------------------------------
    // release — final close of a file handle
    // -----------------------------------------------------------------------
    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let fh_val = fh.0;
        debug!("release: fh={fh_val} start");

        // Flush any remaining writes
        let flush_result = self.flush_writes(fh_val);
        debug!("release: fh={fh_val} flush_result={flush_result:?}");

        // Get ino before removing the handle
        let ino = self.open_files.get(&fh_val).map(|f| f.ino);

        // Remove the file handle
        self.open_files.remove(&fh_val);

        // Decrement per-inode refcount; if last handle, check for deferred cleanup
        if let Some(ino_val) = ino {
            // Clone the Arc out, then drop the DashMap Ref immediately
            // to avoid self-deadlock when we later call .remove()
            let state_arc = self.inode_state.get(&ino_val).map(|r| r.value().clone());

            if let Some(state_ref) = state_arc {
                let remaining = state_ref.release();
                if remaining == 0 {
                    // Last handle closed — check if detached (unlinked while open)
                    let detached = *state_ref.detached.lock();
                    if detached {
                        // Clean up OSS objects now
                        let detached_meta = state_ref.detached_meta.lock().take();
                        if let Some(meta) = detached_meta {
                            if meta.layout.is_some() {
                                let config = self.snap_config();
                                self.rt.block_on(async {
                                    let mut futures = Vec::new();
                                    for ost_info in &config.ost_list {
                                        let addr = ost_info.address.clone();
                                        let ino = ino_val;
                                        futures.push(tokio::spawn(async move {
                                            let _ =
                                                rpc_call(&addr, RpcKind::ObjDeleteInode { ino })
                                                    .await;
                                        }));
                                    }
                                    futures::future::join_all(futures).await;
                                });
                            }
                        }
                        self.inodes.remove(ino_val);
                    }
                    // Clean up per-inode state (DashMap Ref already dropped)
                    self.inode_state.remove(&ino_val);
                }
            }
        }

        reply.ok();
    }

    // -----------------------------------------------------------------------
    // unlink — open-unlink semantics
    // -----------------------------------------------------------------------
    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let path = match self.child_path(parent.0, name) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };

        // Get file metadata
        let meta = match self.stat_path(&path) {
            Ok(m) => m,
            Err(e) => return reply.error(e),
        };

        let ino = meta.ino;
        let has_objects = meta.layout.is_some();
        let nlink = meta.nlink;

        let mds = match self.pick_mds() {
            Ok(m) => m,
            Err(e) => return reply.error(e),
        };

        // MDS unlink first
        let result = self.rt.block_on(async {
            let rpc_reply = rpc_call(&mds, RpcKind::Unlink(path.clone()))
                .await
                .map_err(rustre_err_to_errno)?;
            match rpc_reply.kind {
                RpcKind::Ok => Ok(()),
                RpcKind::Error(e) => Err(mds_error_to_errno(&e)),
                _ => Err(Errno::EIO),
            }
        });

        if let Err(e) = result {
            return reply.error(e);
        }

        if nlink <= 1 {
            // Check if any handle is still open for this inode
            let has_open_handles = self
                .inode_state
                .get(&ino)
                .map(|s| s.refcount.load(Ordering::Relaxed) > 0)
                .unwrap_or(false);

            if has_open_handles {
                // Defer OSS cleanup to release() — open-unlink semantics
                if let Some(state) = self.inode_state.get(&ino) {
                    *state.detached.lock() = true;
                    *state.detached_meta.lock() = Some(meta);
                }
                // Remove from inode path cache so lookups no longer find it
                self.inodes.path_to_ino.remove(&path);
            } else {
                // No open handles — clean up immediately
                if has_objects {
                    let config = self.snap_config();
                    self.rt.block_on(async {
                        let mut futures = Vec::new();
                        for ost_info in &config.ost_list {
                            let addr = ost_info.address.clone();
                            futures.push(tokio::spawn(async move {
                                let _ = rpc_call(&addr, RpcKind::ObjDeleteInode { ino }).await;
                            }));
                        }
                        futures::future::join_all(futures).await;
                    });
                }
                self.inodes.remove(ino);
                self.inode_state.remove(&ino);
            }
        } else {
            // Other links still exist — only remove this path
            self.inodes.path_to_ino.remove(&path);
        }

        reply.ok();
    }

    // -----------------------------------------------------------------------
    // rmdir
    // -----------------------------------------------------------------------
    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let path = match self.child_path(parent.0, name) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };

        let mds = match self.pick_mds() {
            Ok(m) => m,
            Err(e) => return reply.error(e),
        };

        let result = self.rt.block_on(async {
            let rpc_reply = rpc_call(&mds, RpcKind::Unlink(path.clone()))
                .await
                .map_err(rustre_err_to_errno)?;
            match rpc_reply.kind {
                RpcKind::Ok => Ok(()),
                RpcKind::Error(e) => Err(mds_error_to_errno(&e)),
                _ => Err(Errno::EIO),
            }
        });

        match result {
            Ok(()) => {
                if let Some(ino) = self.inodes.path_to_ino.get(&path).map(|r| *r.value()) {
                    self.inodes.remove(ino);
                }
                reply.ok();
            }
            Err(e) => reply.error(e),
        }
    }

    // -----------------------------------------------------------------------
    // rename
    // -----------------------------------------------------------------------
    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        _flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        let old_path = match self.child_path(parent.0, name) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        let new_path = match self.child_path(newparent.0, newname) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };

        let mds = match self.pick_mds() {
            Ok(m) => m,
            Err(e) => return reply.error(e),
        };

        let result = self.rt.block_on(async {
            let reply = rpc_call(
                &mds,
                RpcKind::Rename {
                    old_path: old_path.clone(),
                    new_path: new_path.clone(),
                },
            )
            .await
            .map_err(rustre_err_to_errno)?;
            match reply.kind {
                RpcKind::Ok => Ok(()),
                RpcKind::Error(e) => Err(mds_error_to_errno(&e)),
                _ => Err(Errno::EIO),
            }
        });

        match result {
            Ok(()) => {
                // Update cache
                if let Some(ino) = self.inodes.path_to_ino.get(&old_path).map(|r| *r.value()) {
                    let new_name_str = newname.to_str().unwrap_or("").to_string();
                    self.inodes
                        .rename(ino, &old_path, &new_path, &new_name_str, newparent.0);
                }
                reply.ok();
            }
            Err(e) => reply.error(e),
        }
    }

    // -----------------------------------------------------------------------
    // opendir — snapshot directory entries
    // -----------------------------------------------------------------------
    fn opendir(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let ino_val = ino.0;
        let path = match self.inodes.get_path(ino_val) {
            Some(p) => p,
            None => return reply.error(Errno::ENOENT),
        };

        let mds = match self.pick_mds() {
            Ok(m) => m,
            Err(e) => return reply.error(e),
        };

        let entries = self.rt.block_on(async {
            let rpc_reply = rpc_call(&mds, RpcKind::Readdir(path.clone()))
                .await
                .map_err(rustre_err_to_errno)?;
            match rpc_reply.kind {
                RpcKind::MetaListReply(entries) => Ok(entries),
                RpcKind::Error(e) => Err(mds_error_to_errno(&e)),
                _ => Err(Errno::EIO),
            }
        });

        let entries = match entries {
            Ok(e) => e,
            Err(e) => return reply.error(e),
        };

        // Build full entry list: . and .. first, then children
        let parent_ino = self
            .inodes
            .get_meta(ino_val)
            .map(|m| m.parent_ino)
            .unwrap_or(1);

        let mut all_entries: DirSnapshot = Vec::new();
        all_entries.push((INodeNo(ino_val), FileType::Directory, ".".to_string()));
        all_entries.push((
            INodeNo(if parent_ino == 0 { 1 } else { parent_ino }),
            FileType::Directory,
            "..".to_string(),
        ));

        // Cache children and build entries
        for child in &entries {
            self.inodes.insert(child.clone());
        }

        for entry in &entries {
            let kind = if entry.is_dir {
                FileType::Directory
            } else if entry.symlink_target.is_some() {
                FileType::Symlink
            } else {
                FileType::RegularFile
            };
            all_entries.push((INodeNo(entry.ino), kind, entry.name.clone()));
        }

        let fh = self.alloc_fh();
        self.open_dirs.insert(fh, all_entries);
        reply.opened(FileHandle(fh), FopenFlags::empty());
    }

    // -----------------------------------------------------------------------
    // readdir — refreshes from MDS on offset=0
    // -----------------------------------------------------------------------
    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        // macOS/macFUSE may hold an opendir handle for a long time (e.g.,
        // Finder keeps the root directory open). When offset=0 is requested
        // again, the kernel is starting a fresh listing — re-fetch from MDS
        // so new files/directories created since the original opendir are
        // visible.
        if offset == 0 {
            let ino_val = ino.0;
            if let Some(path) = self.inodes.get_path(ino_val) {
                if let Ok(mds) = self.pick_mds() {
                    let fresh = self.rt.block_on(async {
                        let rpc_reply = rpc_call(&mds, RpcKind::Readdir(path.clone()))
                            .await
                            .map_err(rustre_err_to_errno)?;
                        match rpc_reply.kind {
                            RpcKind::MetaListReply(entries) => Ok(entries),
                            RpcKind::Error(e) => Err(mds_error_to_errno(&e)),
                            _ => Err(Errno::EIO),
                        }
                    });
                    if let Ok(entries) = fresh {
                        let parent_ino = self
                            .inodes
                            .get_meta(ino_val)
                            .map(|m| m.parent_ino)
                            .unwrap_or(1);

                        let mut all_entries: DirSnapshot = Vec::new();
                        all_entries.push((INodeNo(ino_val), FileType::Directory, ".".to_string()));
                        all_entries.push((
                            INodeNo(if parent_ino == 0 { 1 } else { parent_ino }),
                            FileType::Directory,
                            "..".to_string(),
                        ));
                        for child in &entries {
                            self.inodes.insert(child.clone());
                        }
                        for entry in &entries {
                            let kind = if entry.is_dir {
                                FileType::Directory
                            } else if entry.symlink_target.is_some() {
                                FileType::Symlink
                            } else {
                                FileType::RegularFile
                            };
                            all_entries.push((INodeNo(entry.ino), kind, entry.name.clone()));
                        }
                        self.open_dirs.insert(fh.0, all_entries);
                    }
                }
            }
        }

        let snapshot = match self.open_dirs.get(&fh.0) {
            Some(s) => s,
            None => return reply.error(Errno::EBADF),
        };

        for (i, (entry_ino, kind, name)) in snapshot.iter().enumerate().skip(offset as usize) {
            let next_offset = (i + 1) as u64;
            if reply.add(*entry_ino, next_offset, *kind, name) {
                break; // buffer full
            }
        }

        reply.ok();
    }

    // -----------------------------------------------------------------------
    // releasedir
    // -----------------------------------------------------------------------
    fn releasedir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        self.open_dirs.remove(&fh.0);
        reply.ok();
    }

    // -----------------------------------------------------------------------
    // fsyncdir
    // -----------------------------------------------------------------------
    fn fsyncdir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    // -----------------------------------------------------------------------
    // link — hard link
    // -----------------------------------------------------------------------
    fn link(
        &self,
        _req: &Request,
        ino: INodeNo,
        newparent: INodeNo,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let new_path = match self.child_path(newparent.0, newname) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };

        let mds = match self.pick_mds() {
            Ok(m) => m,
            Err(e) => return reply.error(e),
        };

        let result = self.rt.block_on(async {
            let rpc_reply = rpc_call(
                &mds,
                RpcKind::Link {
                    ino: ino.0,
                    new_path: new_path.clone(),
                },
            )
            .await
            .map_err(rustre_err_to_errno)?;
            match rpc_reply.kind {
                RpcKind::MetaReply(m) => Ok(m),
                RpcKind::Error(e) => Err(mds_error_to_errno(&e)),
                _ => Err(Errno::EIO),
            }
        });

        match result {
            Ok(meta) => {
                let attr = meta_to_attr(&meta);
                self.inodes.insert(meta);
                reply.entry(&ENTRY_TTL, &attr, Generation(0));
            }
            Err(e) => reply.error(e),
        }
    }

    // -----------------------------------------------------------------------
    // symlink
    // -----------------------------------------------------------------------
    fn symlink(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        target: &std::path::Path,
        reply: ReplyEntry,
    ) {
        let path = match self.child_path(parent.0, name) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };

        let target_str = match target.to_str() {
            Some(s) => s,
            None => return reply.error(Errno::EINVAL),
        };
        let mds = match self.pick_mds() {
            Ok(m) => m,
            Err(e) => return reply.error(e),
        };

        let result = self.rt.block_on(async {
            let rpc_reply = rpc_call(
                &mds,
                RpcKind::Symlink {
                    path: path.clone(),
                    target: target_str.to_string(),
                },
            )
            .await
            .map_err(rustre_err_to_errno)?;
            match rpc_reply.kind {
                RpcKind::MetaReply(m) => Ok(m),
                RpcKind::Error(e) => Err(mds_error_to_errno(&e)),
                _ => Err(Errno::EIO),
            }
        });

        match result {
            Ok(meta) => {
                let attr = meta_to_attr(&meta);
                self.inodes.insert(meta);
                reply.entry(&ENTRY_TTL, &attr, Generation(0));
            }
            Err(e) => reply.error(e),
        }
    }

    // -----------------------------------------------------------------------
    // readlink
    // -----------------------------------------------------------------------
    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        let meta = match self.inodes.get_meta(ino.0) {
            Some(m) => m,
            None => return reply.error(Errno::ENOENT),
        };

        match meta.symlink_target {
            Some(ref target) => reply.data(target.as_bytes()),
            None => reply.error(Errno::EINVAL),
        }
    }

    // -----------------------------------------------------------------------
    // copy_file_range
    // -----------------------------------------------------------------------
    fn copy_file_range(
        &self,
        _req: &Request,
        ino_in: INodeNo,
        fh_in: FileHandle,
        offset_in: u64,
        ino_out: INodeNo,
        fh_out: FileHandle,
        offset_out: u64,
        len: u64,
        _flags: CopyFileRangeFlags,
        reply: ReplyWrite,
    ) {
        // Read from source
        let src_meta = if let Some(file_ref) = self.open_files.get(&fh_in.0) {
            let mut m = file_ref.meta.clone();
            if let Some(state) = self.inode_state.get(&file_ref.ino) {
                let tracked = state.tracked_size.load(Ordering::Relaxed);
                if tracked > m.size {
                    m.size = tracked;
                }
            }
            m
        } else if let Some(m) = self.inodes.get_meta(ino_in.0) {
            m
        } else {
            return reply.error(Errno::ENOENT);
        };

        let actual_len = std::cmp::min(len, src_meta.size.saturating_sub(offset_in));
        if actual_len == 0 {
            return reply.written(0);
        }

        let data = match self.read_data(&src_meta, offset_in, actual_len as u32) {
            Ok(d) => d,
            Err(e) => return reply.error(e),
        };

        // Write to destination via write_through
        let dest_file = match self.open_files.get(&fh_out.0) {
            Some(f) => f,
            None => return reply.error(Errno::EBADF),
        };

        if !dest_file.writable {
            return reply.error(Errno::EBADF);
        }

        let dest_ino = dest_file.ino;
        let dest_meta = dest_file.meta.clone();
        let new_end = offset_out + data.len() as u64;
        let current_size = self
            .inode_state
            .get(&dest_ino)
            .map(|s| s.tracked_size.load(Ordering::Relaxed))
            .unwrap_or(dest_meta.size);
        let logical_size = std::cmp::max(current_size, new_end);
        drop(dest_file); // Release DashMap ref before write_through

        if let Err(e) = self.write_through(dest_ino, &dest_meta, logical_size, offset_out, &data) {
            return reply.error(e);
        }

        // Update tracked size
        if let Some(state) = self.inode_state.get(&dest_ino) {
            state.update_size(new_end);
            *state.size_dirty.lock() = true;
        }
        self.inodes.update_size(dest_ino, logical_size);

        let _ = ino_out; // Used implicitly through fh_out
        reply.written(data.len() as u32);
    }

    // -----------------------------------------------------------------------
    // ioctl
    // -----------------------------------------------------------------------
    fn ioctl(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _flags: IoctlFlags,
        _cmd: u32,
        _in_data: &[u8],
        _out_size: u32,
        reply: fuser::ReplyIoctl,
    ) {
        reply.error(Errno::ENOTTY);
    }

    // -----------------------------------------------------------------------
    // xattr operations (macOS Finder support)
    // -----------------------------------------------------------------------
    fn listxattr(&self, _req: &Request, _ino: INodeNo, _size: u32, reply: fuser::ReplyXattr) {
        reply.size(0);
    }

    fn getxattr(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _name: &OsStr,
        _size: u32,
        reply: fuser::ReplyXattr,
    ) {
        reply.error(Errno::NO_XATTR);
    }

    fn setxattr(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _name: &OsStr,
        _value: &[u8],
        _flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn removexattr(&self, _req: &Request, _ino: INodeNo, _name: &OsStr, reply: ReplyEmpty) {
        reply.ok();
    }

    // -----------------------------------------------------------------------
    // statfs
    // -----------------------------------------------------------------------
    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        let config = self.snap_config();

        let mut total_bytes: u64 = 0;
        let mut used_bytes: u64 = 0;
        for ost in &config.ost_list {
            total_bytes += ost.total_bytes;
            used_bytes += ost.used_bytes;
        }

        let bsize = BLOCK_SIZE as u64;
        let total_blocks = total_bytes / bsize;
        let free_blocks = (total_bytes.saturating_sub(used_bytes)) / bsize;
        let avail_blocks = free_blocks;

        reply.statfs(
            total_blocks,
            free_blocks,
            avail_blocks,
            0,
            0,
            bsize as u32,
            255,
            bsize as u32,
        );
    }
}
