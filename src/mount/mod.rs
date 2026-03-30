//! FUSE mount — makes Rustre mountable as a POSIX filesystem on macOS/Linux.
//!
//! Architecture:
//!   - `RustreFs` implements `fuser::Filesystem` (async via a dedicated tokio runtime)
//!   - Inode numbers from Rustre MDS are used directly as FUSE inode numbers
//!   - `InodeMap`: two `DashMap`s (entries + path_to_ino) — no outer lock, all methods `&self`
//!   - Open files: `DashMap<u64, OpenFile>` — per-file fine-grained locking
//!   - Write buffering: per-file `parking_lot::Mutex<Vec<(u64, Vec<u8>)>>` for correct
//!     overlapping-write semantics (Finder sends many small writes)
//!   - Config: `parking_lot::RwLock<ClusterConfig>` — always snapshot-clone before block_on
//!
//! Key rule: NEVER hold any lock across `block_on()` — prevents deadlock with
//! the background config refresher task.
//!
//! Reuses existing infrastructure:
//!   - `client::operations::{get_config, mds_addr, ost_addr}` for cluster access
//!   - `rpc::{rpc_call, send_msg, recv_msg}` for MDS/OSS communication
//!   - `types::{FileMeta, StripeLayout, ClusterConfig}` for data types
//!   - Same two-phase commit protocol for writes
//!   - Same MDS-first deletion for unlink/rmdir

use crate::client::operations::{get_config, mds_addr, ost_addr};
use crate::error::RustreError;
use crate::rpc::{recv_msg, rpc_call, send_msg, RpcKind, RpcMessage, MSG_COUNTER};
use crate::types::{ClusterConfig, CreateReq, FileMeta, StripeLayout};
use dashmap::DashMap;
use fuser::{
    AccessFlags, BsdFileFlags, Config, CopyFileRangeFlags, Errno, FileHandle, FopenFlags,
    Generation, INodeNo, InitFlags, IoctlFlags, LockOwner, OpenFlags, RenameFlags, SessionACL,
    TimeOrNow, WriteFlags,
};
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request,
};
use parking_lot::{Mutex, RwLock};
use std::ffi::OsStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tracing::{debug, error, info, warn};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// TTL for cached attributes (FUSE kernel cache)
const ATTR_TTL: Duration = Duration::from_secs(1);
/// TTL for cached directory entries
const ENTRY_TTL: Duration = Duration::from_secs(1);
/// Default stripe count for new files (0 = all OSTs)
const DEFAULT_STRIPE_COUNT: u32 = 0;
/// Default stripe size for new files (1 MiB)
const DEFAULT_STRIPE_SIZE: u64 = 1_048_576;
/// Default block size reported by statfs
const BLOCK_SIZE: u32 = 4096;

// ---------------------------------------------------------------------------
// Error mapping: RustreError → Errno
// ---------------------------------------------------------------------------

fn error_to_errno(e: &RustreError) -> Errno {
    match e {
        RustreError::NotFound(_) => Errno::ENOENT,
        #[cfg(feature = "fdb")]
        RustreError::AlreadyExists(_) => Errno::EEXIST,
        RustreError::IsDirectory(_) => Errno::EISDIR,
        #[cfg(feature = "fdb")]
        RustreError::NotADirectory(_) => Errno::ENOTDIR,
        #[cfg(feature = "fdb")]
        RustreError::DirNotEmpty(_) => Errno::ENOTEMPTY,
        #[cfg(feature = "fdb")]
        RustreError::NoOstAvailable => Errno::ENOSPC,
        #[cfg(feature = "fdb")]
        RustreError::InvalidArgument(_) => Errno::EINVAL,
        RustreError::Io(ref io_err) => Errno::from(io_err.kind()),
        RustreError::Net(_) => Errno::EIO,
        _ => Errno::EIO,
    }
}

fn rustre_err_to_errno(e: RustreError) -> Errno {
    error_to_errno(&e)
}

// ---------------------------------------------------------------------------
// FileMeta → FileAttr conversion
// ---------------------------------------------------------------------------

fn meta_to_attr(meta: &FileMeta) -> FileAttr {
    let kind = if meta.is_dir {
        FileType::Directory
    } else if meta.symlink_target.is_some() {
        FileType::Symlink
    } else {
        FileType::RegularFile
    };
    let ctime = UNIX_EPOCH + Duration::from_secs(meta.ctime);
    let mtime = UNIX_EPOCH + Duration::from_secs(meta.mtime);

    // Calculate block count: number of 512-byte blocks
    let blocks = meta.size.div_ceil(512);

    FileAttr {
        ino: INodeNo(meta.ino),
        size: meta.size,
        blocks,
        atime: mtime, // no separate atime in Rustre, use mtime
        mtime,
        ctime,
        crtime: ctime,
        kind,
        perm: (meta.mode & 0o777) as u16, // Extract permission bits
        nlink: meta.nlink,
        uid: meta.uid,
        gid: meta.gid,
        rdev: 0,
        blksize: BLOCK_SIZE,
        flags: 0,
    }
}

// ---------------------------------------------------------------------------
// Inode cache — lock-free via DashMap
// ---------------------------------------------------------------------------

struct InodeMap {
    /// ino → cached metadata
    entries: DashMap<u64, FileMeta>,
    /// path → ino (for reverse lookups)
    path_to_ino: DashMap<String, u64>,
}

impl InodeMap {
    fn new() -> Self {
        Self {
            entries: DashMap::new(),
            path_to_ino: DashMap::new(),
        }
    }

    fn insert(&self, meta: FileMeta) {
        let ino = meta.ino;
        let path = meta.path.clone();
        self.path_to_ino.insert(path, ino);
        self.entries.insert(ino, meta);
    }

    fn get_meta(&self, ino: u64) -> Option<FileMeta> {
        self.entries.get(&ino).map(|e| e.value().clone())
    }

    fn get_path(&self, ino: u64) -> Option<String> {
        self.entries.get(&ino).map(|e| e.value().path.clone())
    }

    fn update_size(&self, ino: u64, size: u64) {
        if let Some(mut e) = self.entries.get_mut(&ino) {
            e.size = size;
            e.mtime = FileMeta::now_secs();
        }
    }

    fn update_pending(&self, ino: u64, pending: bool) {
        if let Some(mut e) = self.entries.get_mut(&ino) {
            e.pending = pending;
        }
    }

    fn remove(&self, ino: u64) {
        if let Some((_, meta)) = self.entries.remove(&ino) {
            self.path_to_ino.remove(&meta.path);
        }
    }
}

// ---------------------------------------------------------------------------
// Open file handle — per-file state with fine-grained locking
// ---------------------------------------------------------------------------

struct OpenFile {
    ino: u64,
    meta: FileMeta,
    /// Write buffer: append-only list of (offset, data) pairs.
    /// Still used by the legacy flush path and for zero-byte commit cases.
    write_buf: Mutex<Vec<(u64, Vec<u8>)>>,
    /// Current tracked file size (atomic for lock-free reads from getattr)
    tracked_size: AtomicU64,
    /// Whether this file was opened for writing
    writable: bool,
    /// For new files created via create(): true until committed
    pending: bool,
    /// Existing file size changed locally and still needs an MDS SetSize.
    size_dirty: bool,
}

// ---------------------------------------------------------------------------
// RustreFs — the FUSE filesystem implementation
// ---------------------------------------------------------------------------

type OpenDir = Arc<DashMap<u64, Vec<(INodeNo, FileType, String)>>>; // snapshot of directory entries at opendir time

pub struct RustreFs {
    /// Tokio runtime for async operations
    rt: tokio::runtime::Runtime,
    /// Cached cluster configuration (refreshed periodically)
    config: Arc<RwLock<ClusterConfig>>,
    /// Inode cache (lock-free DashMap)
    inodes: Arc<InodeMap>,
    /// Open file handles: fh → OpenFile (lock-free DashMap)
    open_files: Arc<DashMap<u64, OpenFile>>,
    /// Open directory handles: fh → snapshot of entries at opendir time.
    ///
    /// readdir is called repeatedly with increasing offsets.  If we re-fetch
    /// the entry list from MDS every time, concurrent deletes shift the list
    /// and cause offsets to skip entries — making `rm -rf` leave leftover
    /// files.  Snapshotting at opendir keeps the list stable for the whole
    /// readdir sequence.
    open_dirs: OpenDir,
    /// Next file handle number
    next_fh: AtomicU64,
}

impl RustreFs {
    pub fn new(mgs_addr: &str) -> Result<Self, RustreError> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| RustreError::Internal(format!("failed to create tokio runtime: {e}")))?;

        // Fetch initial cluster config
        let config = rt.block_on(get_config(mgs_addr))?;

        // Seed root inode in cache
        let inode_map = InodeMap::new();

        // Query MDS for root to get the real root inode
        let root_meta = rt.block_on(async {
            let mds = mds_addr(&config)?;
            let reply = rpc_call(&mds, RpcKind::Stat("/".to_string())).await?;
            match reply.kind {
                RpcKind::MetaReply(m) => Ok(m),
                RpcKind::Error(e) => Err(RustreError::Net(e)),
                _ => Err(RustreError::Net("unexpected reply".into())),
            }
        })?;

        // FUSE always uses ino=1 for root. If MDS root ino differs, we keep both mappings.
        let mut root = root_meta;
        root.ino = 1; // Force FUSE root ino
        root.path = "/".to_string();
        root.name = "/".to_string();
        inode_map.insert(root);

        info!(
            "RustreFs: initialized with {} MDS, {} OST",
            config.mds_list.len(),
            config.ost_list.len()
        );

        let config = Arc::new(RwLock::new(config));

        // Spawn background config refresher
        let config_clone = Arc::clone(&config);
        let mgs_clone = mgs_addr.to_string();
        rt.spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
                match get_config(&mgs_clone).await {
                    Ok(cfg) => {
                        *config_clone.write() = cfg;
                    }
                    Err(e) => warn!("config refresh failed: {e}"),
                }
            }
        });

        Ok(Self {
            rt,
            config,
            inodes: Arc::new(inode_map),
            open_files: Arc::new(DashMap::new()),
            open_dirs: Arc::new(DashMap::new()),
            next_fh: AtomicU64::new(1),
        })
    }

    // -----------------------------------------------------------------------
    // Internal helpers — snapshot config before block_on to avoid deadlocks
    // -----------------------------------------------------------------------

    /// Snapshot cluster config (clone under brief read lock, then drop lock).
    fn snap_config(&self) -> ClusterConfig {
        self.config.read().clone()
    }

    /// Pick a random MDS address from snapshot.
    fn pick_mds(&self) -> Result<String, Errno> {
        let cfg = self.snap_config();
        mds_addr(&cfg).map_err(rustre_err_to_errno)
    }

    /// Stat a path via MDS, returning FileMeta on success.
    fn stat_path(&self, path: &str) -> Result<FileMeta, Errno> {
        let mds = self.pick_mds()?;
        self.rt.block_on(async {
            let reply = rpc_call(&mds, RpcKind::Stat(path.to_string()))
                .await
                .map_err(rustre_err_to_errno)?;
            match reply.kind {
                RpcKind::MetaReply(m) => Ok(m),
                RpcKind::Error(e) => {
                    if e.contains("not found") {
                        Err(Errno::ENOENT)
                    } else {
                        Err(Errno::EIO)
                    }
                }
                _ => Err(Errno::EIO),
            }
        })
    }

    /// Build child path from parent ino + child name.
    fn child_path(&self, parent: u64, name: &OsStr) -> Result<String, Errno> {
        let name_str = name.to_str().ok_or(Errno::EINVAL)?;
        let parent_path = self.inodes.get_path(parent).ok_or(Errno::ENOENT)?;
        if parent_path == "/" {
            Ok(format!("/{name_str}"))
        } else {
            Ok(format!("{parent_path}/{name_str}"))
        }
    }

    /// Read data from OSTs for a specific byte range of a file.
    fn read_data(&self, meta: &FileMeta, offset: u64, size: u32) -> Result<Vec<u8>, Errno> {
        let layout = meta.layout.as_ref().ok_or(Errno::EIO)?;
        let file_size = meta.size;
        let config = self.snap_config();

        // Clamp read to file bounds
        if offset >= file_size {
            return Ok(Vec::new());
        }
        let read_end = std::cmp::min(offset + size as u64, file_size);
        let read_len = (read_end - offset) as usize;

        let chunk_size = layout.stripe_size;

        // Determine which chunks we need
        let first_chunk = (offset / chunk_size) as u32;
        let last_chunk = ((read_end - 1) / chunk_size) as u32;

        let mut result = vec![0u8; read_len];

        self.rt.block_on(async {
            let mut read_handles = Vec::new();

            // Create a task for each chunk to read in parallel
            for chunk_idx in first_chunk..=last_chunk {
                let chunk_file_offset = chunk_idx as u64 * chunk_size;
                let chunk_end = std::cmp::min(chunk_file_offset + chunk_size, file_size);

                // What portion of this chunk do we need?
                let read_start_in_chunk = if offset > chunk_file_offset {
                    (offset - chunk_file_offset) as usize
                } else {
                    0
                };
                let read_end_in_chunk = if read_end < chunk_end {
                    (read_end - chunk_file_offset) as usize
                } else {
                    (chunk_end - chunk_file_offset) as usize
                };

                // Where does this go in our result buffer?
                let buf_start = if chunk_file_offset > offset {
                    (chunk_file_offset - offset) as usize
                } else {
                    0
                };

                // Fetch the whole chunk from OST in parallel
                let object_id = StripeLayout::object_id(meta.ino, chunk_idx);
                let primary_ost = layout.ost_for_chunk(chunk_idx);
                let addr = ost_addr(&config, primary_ost).map_err(rustre_err_to_errno)?;
                let addr = addr.clone();

                read_handles.push((
                    chunk_idx,
                    buf_start,
                    read_start_in_chunk,
                    read_end_in_chunk,
                    tokio::spawn(async move { read_chunk_from_ost(&addr, &object_id).await }),
                ));
            }

            // Wait for all reads to complete and copy data into result buffer
            for (chunk_idx, buf_start, read_start_in_chunk, read_end_in_chunk, handle) in
                read_handles
            {
                match handle.await {
                    Ok(Ok(chunk_data)) => {
                        if chunk_data.len() >= read_end_in_chunk {
                            let src = &chunk_data[read_start_in_chunk..read_end_in_chunk];
                            let dst = &mut result[buf_start..buf_start + src.len()];
                            dst.copy_from_slice(src);
                        }
                    }
                    Ok(Err(e)) => return Err(e),
                    Err(e) => {
                        error!("read task panicked for chunk {}: {}", chunk_idx, e);
                        return Err(Errno::EIO);
                    }
                }
            }

            Ok(result)
        })
    }

    fn sync_size_to_mds(&self, meta: &FileMeta, size: u64) -> Result<(), Errno> {
        let mds = self.pick_mds()?;
        self.rt.block_on(async {
            let rpc_reply = rpc_call(
                &mds,
                RpcKind::SetSize {
                    path: meta.path.clone(),
                    size,
                },
            )
            .await
            .map_err(rustre_err_to_errno)?;
            match rpc_reply.kind {
                RpcKind::Ok => Ok(()),
                RpcKind::Error(e) => {
                    error!("setsize failed: {e}");
                    Err(Errno::EIO)
                }
                _ => Err(Errno::EIO),
            }
        })
    }

    /// Write a single userspace write request through to the relevant OST chunks.
    ///
    /// This makes newly written bytes visible before close()/flush(), which is
    /// critical for toolchains that reopen, mmap, or back-patch their outputs
    /// while the same file handle is still alive.
    fn write_through(
        &self,
        ino: u64,
        meta: &FileMeta,
        logical_size: u64,
        offset: u64,
        data: &[u8],
    ) -> Result<(), Errno> {
        if data.is_empty() {
            return Ok(());
        }

        let layout = meta.layout.as_ref().ok_or(Errno::EIO)?;
        let config = self.snap_config();
        let chunk_size = layout.stripe_size;
        let write_end = offset + data.len() as u64;
        let first_chunk = (offset / chunk_size) as u32;
        let last_chunk = ((write_end - 1) / chunk_size) as u32;
        let layout_clone = layout.clone();
        let config_clone = config.clone();

        self.rt.block_on(async move {
            let mut write_handles = Vec::new();

            for chunk_idx in first_chunk..=last_chunk {
                let chunk_file_start = chunk_idx as u64 * chunk_size;
                let chunk_file_end = chunk_file_start + chunk_size;
                let chunk_logical_end = std::cmp::min(logical_size, chunk_file_end);
                let logical_len = chunk_logical_end.saturating_sub(chunk_file_start) as usize;

                let write_start = std::cmp::max(offset, chunk_file_start);
                let write_end = std::cmp::min(offset + data.len() as u64, chunk_file_end);
                if write_start >= write_end {
                    continue;
                }

                let object_id = StripeLayout::object_id(ino, chunk_idx);
                let ost_index = layout_clone.ost_for_chunk(chunk_idx);
                let addr = ost_addr(&config_clone, ost_index).map_err(rustre_err_to_errno)?;

                // Check if we're writing the entire chunk from start to end
                let writes_entire_chunk =
                    write_start == chunk_file_start && write_end == chunk_file_end;

                if writes_entire_chunk {
                    // For entire chunk writes, we can write directly without reading existing data
                    let chunk_data = data
                        [(write_start - offset) as usize..(write_end - offset) as usize]
                        .to_vec();

                    let addr_clone = addr.clone();
                    write_handles.push(tokio::spawn(async move {
                        write_chunk_to_ost(&addr_clone, &object_id, &chunk_data).await
                    }));
                } else {
                    // For partial writes, we need to read-merge-write
                    let existing = read_chunk_from_ost(&addr, &object_id).await?;
                    let target_len = std::cmp::max(existing.len(), logical_len);
                    let mut chunk_data = vec![0u8; target_len];
                    chunk_data[..existing.len()].copy_from_slice(&existing);

                    let dst_start = (write_start - chunk_file_start) as usize;
                    let dst_end = (write_end - chunk_file_start) as usize;
                    let src_start = (write_start - offset) as usize;
                    let src_end = (write_end - offset) as usize;
                    chunk_data[dst_start..dst_end].copy_from_slice(&data[src_start..src_end]);

                    let addr_clone = addr.clone();
                    write_handles.push(tokio::spawn(async move {
                        write_chunk_to_ost(&addr_clone, &object_id, &chunk_data).await
                    }));
                }
            }

            // Wait for all writes to complete
            for handle in write_handles {
                handle.await.map_err(|_| Errno::EIO)??;
            }

            Ok(())
        })
    }

    /// Flush write buffers for an open file — performs the two-phase commit.
    fn flush_writes(&self, fh: u64) -> Result<(), Errno> {
        // Get file info from the DashMap — take write_buf under per-file lock
        let (ino, meta, write_buf, pending, size_dirty) = {
            let file_ref = self.open_files.get(&fh).ok_or(Errno::EBADF)?;
            let file = file_ref.value();
            let buf = {
                let mut wb = file.write_buf.lock();
                std::mem::take(&mut *wb)
            };
            let is_empty = buf.is_empty();
            if is_empty && !file.pending && !file.size_dirty {
                return Ok(());
            }
            (
                file.ino,
                file.meta.clone(),
                buf,
                file.pending,
                file.size_dirty,
            )
        };

        if write_buf.is_empty() {
            if pending {
                self.rt.block_on(async {
                    let mds = self.pick_mds()?;
                    let reply = rpc_call(
                        &mds,
                        RpcKind::CommitCreate {
                            ino,
                            size: meta.size,
                        },
                    )
                    .await
                    .map_err(rustre_err_to_errno)?;
                    match reply.kind {
                        RpcKind::Ok => Ok(()),
                        RpcKind::Error(e) => {
                            error!("commit failed: {e}");
                            Err(Errno::EIO)
                        }
                        _ => Err(Errno::EIO),
                    }
                })?;
                if let Some(mut file_ref) = self.open_files.get_mut(&fh) {
                    file_ref.pending = false;
                    file_ref.size_dirty = false;
                }
                self.inodes.update_pending(ino, false);
            } else if size_dirty {
                self.sync_size_to_mds(&meta, meta.size)?;
                if let Some(mut file_ref) = self.open_files.get_mut(&fh) {
                    file_ref.size_dirty = false;
                }
                self.inodes.update_size(ino, meta.size);
            }
            return Ok(());
        }

        let layout = meta.layout.as_ref().ok_or(Errno::EIO)?;
        let config = self.snap_config();
        let chunk_size = layout.stripe_size as usize;

        // Compute the byte range covered by this write buffer.
        // We build a flat contiguous Vec<u8> and apply writes in order
        // (later writes at the same offset naturally overwrite earlier ones).
        let mut min_off = u64::MAX;
        let mut max_end: u64 = 0;
        for (off, data) in &write_buf {
            if *off < min_off {
                min_off = *off;
            }
            let end = *off + data.len() as u64;
            if end > max_end {
                max_end = end;
            }
        }

        // max_offset = actual file size after this flush
        let max_offset = std::cmp::max(meta.size, max_end);

        // Build a flat buffer spanning [min_off..max_end), zero-initialized.
        // Then overlay each write operation in order.
        let buf_len = (max_end - min_off) as usize;
        let mut flat_buf = vec![0u8; buf_len];
        for (off, data) in &write_buf {
            let start = (*off - min_off) as usize;
            flat_buf[start..start + data.len()].copy_from_slice(data);
        }

        debug!(
            "flush_writes: ino={ino:#x} pending={pending} meta.path={} \
             writes={} bytes=[{min_off}..{max_end}) max_offset={max_offset} \
             magic={:02x?}",
            meta.path,
            write_buf.len(),
            &flat_buf[..std::cmp::min(4, flat_buf.len())],
        );

        // Determine which chunks are touched by the write range [min_off..max_end)
        let first_chunk = (min_off / chunk_size as u64) as u32;
        let last_chunk = if max_end == 0 {
            0
        } else {
            ((max_end - 1) / chunk_size as u64) as u32
        };

        let layout_clone = layout.clone();
        let config_clone = config.clone();

        // Write each affected chunk to the appropriate OST
        self.rt.block_on(async {
            let mut write_handles = Vec::new();

            for chunk_idx in first_chunk..=last_chunk {
                let chunk_file_start = chunk_idx as u64 * chunk_size as u64;
                let chunk_file_end = chunk_file_start + chunk_size as u64;

                // The actual byte range of this chunk within the file
                let actual_chunk_end = std::cmp::min(chunk_file_end, max_offset);
                let actual_chunk_size = (actual_chunk_end - chunk_file_start) as usize;

                // Build the chunk data
                let mut chunk_data = vec![0u8; actual_chunk_size];

                // Copy the written bytes that fall into this chunk
                let write_start_in_file = std::cmp::max(chunk_file_start, min_off);
                let write_end_in_file = std::cmp::min(chunk_file_end, max_end);

                if write_start_in_file < write_end_in_file {
                    let dst_start = (write_start_in_file - chunk_file_start) as usize;
                    let dst_end = (write_end_in_file - chunk_file_start) as usize;
                    let src_start = (write_start_in_file - min_off) as usize;
                    let src_end = (write_end_in_file - min_off) as usize;
                    chunk_data[dst_start..dst_end].copy_from_slice(&flat_buf[src_start..src_end]);
                }

                // If this chunk is only partially written and the file already had data
                // at this offset, we need to read-merge with existing OST data so we
                // don't zero out bytes we didn't touch.
                let chunk_fully_covered =
                    min_off <= chunk_file_start && max_end >= actual_chunk_end;
                if !chunk_fully_covered && meta.size > chunk_file_start {
                    let object_id = StripeLayout::object_id(ino, chunk_idx);
                    let ost_index = layout_clone.ost_for_chunk(chunk_idx);
                    let addr = ost_addr(&config_clone, ost_index).map_err(rustre_err_to_errno)?;
                    if let Ok(existing) = read_chunk_from_ost(&addr, &object_id).await {
                        if !existing.is_empty() {
                            // Merge: fill in only the bytes NOT covered by our write
                            for i in 0..std::cmp::min(existing.len(), actual_chunk_size) {
                                let file_off = chunk_file_start + i as u64;
                                if file_off < min_off || file_off >= max_end {
                                    // This byte is outside our write range — keep existing
                                    chunk_data[i] = existing[i];
                                }
                            }
                        }
                    }
                }

                let object_id = StripeLayout::object_id(ino, chunk_idx);
                let ost_index = layout_clone.ost_for_chunk(chunk_idx);
                let addr = ost_addr(&config_clone, ost_index).map_err(rustre_err_to_errno)?;
                let addr = addr.clone();

                write_handles.push(tokio::spawn(async move {
                    write_chunk_to_ost(&addr, &object_id, &chunk_data).await
                }));
            }

            // Wait for all writes
            for handle in write_handles {
                handle
                    .await
                    .map_err(|_| Errno::EIO)?
                    .map_err(|_| Errno::EIO)?;
            }

            // Update MDS metadata:
            // - For pending files (new via create()): CommitCreate to set size + make visible
            // - For existing files (opened via open()): SetSize to update size
            let mds = self.pick_mds()?;
            let rpc_kind = if pending {
                RpcKind::CommitCreate {
                    ino,
                    size: max_offset,
                }
            } else {
                RpcKind::SetSize {
                    path: meta.path.clone(),
                    size: max_offset,
                }
            };
            let reply = rpc_call(&mds, rpc_kind)
                .await
                .map_err(rustre_err_to_errno)?;

            match reply.kind {
                RpcKind::Ok => {}
                RpcKind::Error(e) => {
                    error!("MDS size update failed: {e}");
                    return Err(Errno::EIO);
                }
                _ => return Err(Errno::EIO),
            }

            Ok(())
        })?;

        // Update cached metadata
        if let Some(mut file_ref) = self.open_files.get_mut(&fh) {
            file_ref.meta.size = max_offset;
            file_ref.size_dirty = false;
            if pending {
                file_ref.meta.pending = false;
                file_ref.pending = false;
            }
            file_ref.tracked_size.store(max_offset, Ordering::Relaxed);
        }
        self.inodes.update_size(ino, max_offset);
        if pending {
            self.inodes.update_pending(ino, false);
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// OST I/O helpers (async, called from block_on)
// ---------------------------------------------------------------------------

/// Read an entire chunk from an OST.
async fn read_chunk_from_ost(addr: &str, object_id: &str) -> Result<Vec<u8>, Errno> {
    let mut stream = TcpStream::connect(addr).await.map_err(|_| Errno::EIO)?;

    let request = RpcMessage {
        id: MSG_COUNTER.fetch_add(1, Ordering::Relaxed),
        kind: RpcKind::ObjReadZeroCopy {
            object_id: object_id.to_string(),
            length: 0, // 0 = read entire object
        },
    };
    send_msg(&mut stream, &request)
        .await
        .map_err(|_| Errno::EIO)?;

    let reply = recv_msg(&mut stream).await.map_err(|_| Errno::EIO)?;

    match reply.kind {
        RpcKind::DataReply(length_bytes) => {
            let bytes: [u8; 8] = length_bytes.as_slice().try_into().map_err(|_| Errno::EIO)?;
            let length = usize::from_le_bytes(bytes);
            let mut data = vec![0u8; length];
            stream.read_exact(&mut data).await.map_err(|_| Errno::EIO)?;
            Ok(data)
        }
        RpcKind::Error(e) => {
            if e.contains("not found") {
                // Chunk doesn't exist — return zeros (sparse file support)
                Ok(Vec::new())
            } else {
                Err(Errno::EIO)
            }
        }
        _ => Err(Errno::EIO),
    }
}

/// Write a chunk to an OST.
async fn write_chunk_to_ost(addr: &str, object_id: &str, data: &[u8]) -> Result<(), Errno> {
    let mut stream = TcpStream::connect(addr).await.map_err(|_| Errno::EIO)?;

    let request = RpcMessage {
        id: MSG_COUNTER.fetch_add(1, Ordering::Relaxed),
        kind: RpcKind::ObjWriteZeroCopy {
            object_id: object_id.to_string(),
            length: data.len(),
        },
    };
    send_msg(&mut stream, &request)
        .await
        .map_err(|_| Errno::EIO)?;

    // Send data after the RPC header
    use tokio::io::AsyncWriteExt;
    stream.write_all(data).await.map_err(|_| Errno::EIO)?;
    stream.flush().await.map_err(|_| Errno::EIO)?;

    let reply = recv_msg(&mut stream).await.map_err(|_| Errno::EIO)?;
    match reply.kind {
        RpcKind::Ok => Ok(()),
        RpcKind::Error(e) => {
            error!("OST write failed: {e}");
            Err(Errno::EIO)
        }
        _ => Err(Errno::EIO),
    }
}

// ---------------------------------------------------------------------------
// Filesystem trait implementation
// ---------------------------------------------------------------------------

impl Filesystem for RustreFs {
    // -- init: called when the filesystem is mounted --
    fn init(
        &mut self,
        _req: &Request,
        config: &mut fuser::KernelConfig,
    ) -> Result<(), std::io::Error> {
        // If the kernel supports mmap on direct-IO files, opt in. We return
        // FOPEN_DIRECT_IO only for writable handles to avoid macFUSE buffered-write
        // corruption, and this keeps mmap-based writers working when supported.
        let _ = config.add_capabilities(InitFlags::FUSE_DIRECT_IO_ALLOW_MMAP);

        info!("RustreFs: FUSE filesystem initialized");
        Ok(())
    }

    // -- access: check file accessibility --
    fn access(&self, _req: &Request, ino: INodeNo, _mask: AccessFlags, reply: ReplyEmpty) {
        // Check if inode exists in cache
        if self.inodes.get_meta(ino.0).is_some() {
            reply.ok();
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    // -- rename --
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
                RpcKind::Error(e) => {
                    if e.contains("not found") {
                        Err(Errno::ENOENT)
                    } else if e.contains("already exists") {
                        Err(Errno::EEXIST)
                    } else if e.contains("not a directory") {
                        Err(Errno::ENOTDIR)
                    } else if e.contains("directory not empty") {
                        Err(Errno::ENOTEMPTY)
                    } else if e.contains("invalid argument") {
                        Err(Errno::EINVAL)
                    } else {
                        Err(Errno::EIO)
                    }
                }
                _ => Err(Errno::EIO),
            }
        });

        match result {
            Ok(()) => {
                // Update cache: remove old path, add new path
                if let Some(ino) = self.inodes.path_to_ino.get(&old_path).map(|r| *r.value()) {
                    self.inodes.path_to_ino.remove(&old_path);
                    self.inodes.path_to_ino.insert(new_path.clone(), ino);
                    if let Some(mut entry) = self.inodes.entries.get_mut(&ino) {
                        entry.path = new_path;
                        entry.name = newname.to_str().unwrap_or("").to_string();
                        entry.parent_ino = newparent.0;
                        entry.mtime = FileMeta::now_secs();
                    }
                }
                reply.ok();
            }
            Err(e) => reply.error(e),
        }
    }

    // -- lookup: resolve parent + name → child attributes --
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

    // -- getattr: get file attributes by ino --
    fn getattr(&self, _req: &Request, ino: INodeNo, fh: Option<FileHandle>, reply: ReplyAttr) {
        // If we have a file handle, check if the open file has a more up-to-date size
        if let Some(fh_val) = fh {
            if let Some(file_ref) = self.open_files.get(&fh_val.0) {
                let file = file_ref.value();
                let mut meta = file.meta.clone();
                let tracked = file.tracked_size.load(Ordering::Relaxed);
                if tracked > meta.size {
                    meta.size = tracked;
                }
                return reply.attr(&ATTR_TTL, &meta_to_attr(&meta));
            }
        }

        // Try inode cache
        if let Some(meta) = self.inodes.get_meta(ino.0) {
            return reply.attr(&ATTR_TTL, &meta_to_attr(&meta));
        }

        // Cache miss — for unknown inodes, return ENOENT (they'll get cached via lookup)
        reply.error(Errno::ENOENT);
    }

    // -- setattr: handle truncate, mode, timestamps --
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

        // Handle permission changes (mode, uid, gid)
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
                    RpcKind::Error(e) => {
                        error!("setperms failed: {e}");
                        Err(Errno::EIO)
                    }
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
            // First try explicit file handle
            let maybe_fh = fh.and_then(|fh_val| self.open_files.get(&fh_val.0).map(|_| fh_val.0));

            // If no fh provided (or fh not found), search by ino
            let effective_fh = maybe_fh.or_else(|| {
                self.open_files.iter().find_map(|entry| {
                    if entry.value().ino == ino_val {
                        Some(*entry.key())
                    } else {
                        None
                    }
                })
            });

            // Check if the open file is pending (created via create(), not yet committed).
            // Pending files should NEVER call MDS SetSize — their final size is set
            // atomically by CommitCreate when flush_writes runs.
            //
            // This handles three patterns:
            //   1. Finder:  create() → write() → setattr(size=0) → flush()
            //   2. Linker:  create() → setattr(size=N) → write() → flush()
            //   3. O_TRUNC: open() → setattr(size=0) → write() → flush()
            //
            // Cases 1 & 2: file.pending=true → local-only update
            // Case 3:       file.pending=false → must call MDS SetSize
            let is_pending = effective_fh
                .and_then(|fh_val| self.open_files.get(&fh_val))
                .map(|file_ref| file_ref.value().pending)
                .unwrap_or(false);

            debug!(
                "setattr: ino={ino_val:#x} size={new_size} fh={fh:?} \
                 effective_fh={effective_fh:?} is_pending={is_pending}",
            );

            if is_pending {
                // Pending file: just update local state.
                // flush_writes → CommitCreate will set the final size on MDS.
                if let Some(fh_val) = effective_fh {
                    if let Some(mut file_ref) = self.open_files.get_mut(&fh_val) {
                        let file = file_ref.value_mut();
                        file.tracked_size.store(new_size, Ordering::Relaxed);
                        file.meta.size = new_size;
                    }
                }
                self.inodes.update_size(ino_val, new_size);
            } else {
                // Committed file (O_TRUNC, normal truncate, etc.):
                // must call MDS to actually truncate/resize.
                if let Some(fh_val) = effective_fh {
                    if let Some(mut file_ref) = self.open_files.get_mut(&fh_val) {
                        let file = file_ref.value_mut();
                        file.tracked_size.store(new_size, Ordering::Relaxed);
                        file.meta.size = new_size;
                    }
                }
                self.inodes.update_size(ino_val, new_size);
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
                        RpcKind::SetSize {
                            path: path.clone(),
                            size: new_size,
                        },
                    )
                    .await
                    .map_err(rustre_err_to_errno)?;
                    match rpc_reply.kind {
                        RpcKind::Ok => Ok(()),
                        RpcKind::Error(e) => {
                            error!("setsize failed: {e}");
                            Err(Errno::EIO)
                        }
                        _ => Err(Errno::EIO),
                    }
                });

                if let Err(e) = result {
                    return reply.error(e);
                }

                if let Some(fh_val) = effective_fh {
                    if let Some(mut file_ref) = self.open_files.get_mut(&fh_val) {
                        file_ref.size_dirty = false;
                    }
                }

                // Update cache (redundant but harmless)
                self.inodes.update_size(ino_val, new_size);
            }
        }

        // Return current attributes
        match self.inodes.get_meta(ino_val) {
            Some(meta) => reply.attr(&ATTR_TTL, &meta_to_attr(&meta)),
            None => reply.error(Errno::ENOENT),
        }
    }

    // -- readdir: list directory contents --
    //
    // Uses the snapshot captured at opendir time so that concurrent deletes
    // (e.g. `rm -rf`) don't shift the entry list and cause offset-based
    // iteration to skip entries.
    fn readdir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
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

    // -- opendir: open a directory and snapshot its contents --
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

        // Fetch directory entries from MDS and snapshot them for this handle.
        let entries = self.rt.block_on(async {
            let rpc_reply = rpc_call(&mds, RpcKind::Readdir(path.clone()))
                .await
                .map_err(rustre_err_to_errno)?;
            match rpc_reply.kind {
                RpcKind::MetaListReply(entries) => Ok(entries),
                RpcKind::Error(e) => {
                    if e.contains("not found") {
                        Err(Errno::ENOENT)
                    } else if e.contains("not a directory") {
                        Err(Errno::ENOTDIR)
                    } else {
                        Err(Errno::EIO)
                    }
                }
                _ => Err(Errno::EIO),
            }
        });

        let entries = match entries {
            Ok(e) => e,
            Err(e) => return reply.error(e),
        };

        // Build the full entry list: . and .. first, then children
        let parent_ino = self
            .inodes
            .get_meta(ino_val)
            .map(|m| m.parent_ino)
            .unwrap_or(1);

        let mut all_entries: Vec<(INodeNo, FileType, String)> = Vec::new();
        all_entries.push((INodeNo(ino_val), FileType::Directory, ".".to_string()));
        all_entries.push((
            INodeNo(if parent_ino == 0 { 1 } else { parent_ino }),
            FileType::Directory,
            "..".to_string(),
        ));

        // Cache children in inode map and build entries
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

        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        self.open_dirs.insert(fh, all_entries);
        reply.opened(FileHandle(fh), FopenFlags::empty());
    }

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

    fn fsyncdir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        // Directories don't need syncing in our architecture (MDS uses FDB transactions)
        reply.ok();
    }

    // -- mkdir: create directory --
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

        // Apply umask to mode
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
                RpcKind::Error(e) => {
                    if e.contains("already exists") {
                        Err(Errno::EEXIST)
                    } else if e.contains("not found") {
                        Err(Errno::ENOENT)
                    } else {
                        Err(Errno::EIO)
                    }
                }
                _ => Err(Errno::EIO),
            }
        });

        if let Err(e) = result {
            return reply.error(e);
        }

        // Stat the new directory to get its metadata
        match self.stat_path(&path) {
            Ok(meta) => {
                let attr = meta_to_attr(&meta);
                self.inodes.insert(meta);
                reply.entry(&ENTRY_TTL, &attr, Generation(0));
            }
            Err(e) => reply.error(e),
        }
    }

    // -- unlink: remove a file --
    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let path = match self.child_path(parent.0, name) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };

        // Get file metadata first (need ino + nlink for deciding OSS cleanup)
        let meta = match self.stat_path(&path) {
            Ok(m) => m,
            Err(e) => return reply.error(e),
        };

        let ino = meta.ino;
        let has_objects = meta.layout.is_some();
        let nlink = meta.nlink;
        let config = self.snap_config();

        let mds = match self.pick_mds() {
            Ok(m) => m,
            Err(e) => return reply.error(e),
        };

        // MDS unlink first (MDS handles nlink decrement vs full removal)
        let result = self.rt.block_on(async {
            let rpc_reply = rpc_call(&mds, RpcKind::Unlink(path.clone()))
                .await
                .map_err(rustre_err_to_errno)?;
            match rpc_reply.kind {
                RpcKind::Ok => Ok(()),
                RpcKind::Error(e) => {
                    if e.contains("not found") {
                        Err(Errno::ENOENT)
                    } else {
                        Err(Errno::EIO)
                    }
                }
                _ => Err(Errno::EIO),
            }
        });

        if let Err(e) = result {
            return reply.error(e);
        }

        if nlink <= 1 {
            // Last link removed — clean up OSS objects and inode cache
            if has_objects {
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
            // Remove inode from cache entirely
            self.inodes.remove(ino);
        } else {
            // Other links still exist — only remove this path from cache
            self.inodes.path_to_ino.remove(&path);
        }
        reply.ok();
    }

    // -- rmdir: remove a directory --
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
                RpcKind::Error(e) => {
                    if e.contains("not found") {
                        Err(Errno::ENOENT)
                    } else if e.contains("not empty") {
                        Err(Errno::ENOTEMPTY)
                    } else {
                        Err(Errno::EIO)
                    }
                }
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

    // -- open: open a file, return a file handle --
    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let ino_val = ino.0;
        let mut meta = match self.inodes.get_meta(ino_val) {
            Some(m) => m,
            None => {
                return reply.error(Errno::ENOENT);
            }
        };

        let raw_flags = flags.0;
        let writable = (raw_flags & libc::O_WRONLY != 0) || (raw_flags & libc::O_RDWR != 0);
        let truncate = raw_flags & libc::O_TRUNC != 0;

        // Handle O_TRUNC: truncate the file to zero length
        if truncate && writable && meta.size > 0 {
            let mds = match self.pick_mds() {
                Ok(m) => m,
                Err(e) => return reply.error(e),
            };
            let path = meta.path.clone();
            let result = self.rt.block_on(async {
                let rpc_reply = rpc_call(&mds, RpcKind::SetSize { path, size: 0 })
                    .await
                    .map_err(rustre_err_to_errno)?;
                match rpc_reply.kind {
                    RpcKind::Ok => Ok(()),
                    RpcKind::Error(e) => {
                        error!("truncate failed: {e}");
                        Err(Errno::EIO)
                    }
                    _ => Err(Errno::EIO),
                }
            });
            if let Err(e) = result {
                return reply.error(e);
            }
            meta.size = 0;
            self.inodes.update_size(ino_val, 0);
        }

        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);

        debug!(
            "open: ino={ino_val:#x} fh={fh} flags={raw_flags:#x} writable={writable} \
             truncate={truncate} meta.size={} path={}",
            meta.size, meta.path,
        );

        let open_file = OpenFile {
            ino: ino_val,
            tracked_size: AtomicU64::new(meta.size),
            meta,
            write_buf: Mutex::new(Vec::new()),
            writable,
            pending: false,
            size_dirty: false,
        };

        self.open_files.insert(fh, open_file);

        // macFUSE buffered writes on writable handles can corrupt linker/rustc outputs
        // (replayed/overlapping writes with stale zero pages). Force direct I/O for
        // writable handles so each write request carries the real userspace buffer.
        // Keep read-only handles cached so exec/mmap of finished outputs still works.
        let mut open_flags = FopenFlags::empty();
        if writable {
            open_flags |= FopenFlags::FOPEN_DIRECT_IO;
        }
        reply.opened(FileHandle(fh), open_flags);
    }

    // -- read: read file data --
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
        let fh_val = fh.0;

        // If this fd has buffered writes, flush them first to ensure read-after-write
        // consistency (e.g., build scripts that write then read the same file).
        if let Some(file_ref) = self.open_files.get(&fh_val) {
            let has_buffered = !file_ref.write_buf.lock().is_empty();
            drop(file_ref);
            if has_buffered {
                let _ = self.flush_writes(fh_val);
            }
        }

        // Get metadata from open file handle or inode cache
        let meta = if let Some(file_ref) = self.open_files.get(&fh_val) {
            let mut m = file_ref.meta.clone();
            let tracked = file_ref.tracked_size.load(Ordering::Relaxed);
            if tracked > m.size {
                m.size = tracked;
            }
            m
        } else if let Some(m) = self.inodes.get_meta(ino_val) {
            m
        } else {
            return reply.error(Errno::ENOENT);
        };

        if meta.is_dir {
            return reply.error(Errno::EISDIR);
        }

        // Symlinks don't have data on OSTs
        if meta.symlink_target.is_some() {
            return reply.error(Errno::EINVAL);
        }

        match self.read_data(&meta, offset, size) {
            Ok(data) => reply.data(&data),
            Err(e) => reply.error(e),
        }
    }

    // -- create: create and open a new file (atomic) --
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

        // Apply umask to mode
        let effective_mode = mode & !umask;
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };

        // Create pending file on MDS
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
                RpcKind::Error(e) => {
                    if e.contains("already exists") {
                        Err(Errno::EEXIST)
                    } else if e.contains("not found") {
                        Err(Errno::ENOENT)
                    } else {
                        Err(Errno::EIO)
                    }
                }
                _ => Err(Errno::EIO),
            }
        });

        let meta = match meta {
            Ok(m) => m,
            Err(e) => return reply.error(e),
        };

        let ino = meta.ino;

        // Immediately commit the pending file so it's visible to lookups and
        // readdir right away.  Without this, tools like `git init` (via libgit2)
        // fail because they create a file, then stat/readdir the parent, and
        // MDS filters out pending entries.
        //
        // The file starts at size 0.  Writes go through write_through() directly
        // to OST, and the final size is synced to MDS on flush/release via
        // SetSize (the normal non-pending path).
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
            // Best-effort abort the pending MDS record
            self.rt.block_on(async {
                let mds_addr = self.pick_mds().ok();
                if let Some(addr) = mds_addr {
                    let _ = rpc_call(&addr, RpcKind::AbortCreate { ino }).await;
                }
            });
            return reply.error(e);
        }

        // Update meta to reflect committed state
        let mut meta = meta;
        meta.pending = false;

        let attr = meta_to_attr(&meta);
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);

        // Cache the inode (committed, visible)
        self.inodes.insert(meta.clone());

        // Create open file handle — NOT pending, so writes mark size_dirty
        // and flush/release syncs size via SetSize.
        let open_file = OpenFile {
            ino,
            tracked_size: AtomicU64::new(0),
            meta,
            write_buf: Mutex::new(Vec::new()),
            writable: true,
            pending: false,
            size_dirty: false,
        };

        self.open_files.insert(fh, open_file);

        debug!("create: ino={ino:#x} fh={fh} path={}", attr.ino);

        // Same rationale as open(): force direct I/O for newly created writable files
        // so compiler/linker outputs don't go through macFUSE's problematic buffered
        // write path.
        reply.created(
            &ENTRY_TTL,
            &attr,
            Generation(0),
            FileHandle(fh),
            FopenFlags::FOPEN_DIRECT_IO,
        );
    }

    // -- write: write through to OSS so in-flight outputs are readable/patchable --
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
        let (ino, meta, pending, current_size) = match self.open_files.get(&fh_val) {
            Some(file_ref) => {
                let file = file_ref.value();
                if !file.writable {
                    return reply.error(Errno::EBADF);
                }
                // Can't write to symlinks
                if file.meta.symlink_target.is_some() {
                    return reply.error(Errno::EINVAL);
                }
                (
                    file.ino,
                    file.meta.clone(),
                    file.pending,
                    std::cmp::max(file.meta.size, file.tracked_size.load(Ordering::Relaxed)),
                )
            }
            None => return reply.error(Errno::EBADF),
        };

        let new_end = offset + data.len() as u64;
        let logical_size = std::cmp::max(current_size, new_end);

        // Determine if this write covers one or more complete chunks
        let layout = match meta.layout.as_ref() {
            Some(l) => l,
            None => {
                // No layout (shouldn't happen for a file with data)
                return reply.error(Errno::EIO);
            }
        };

        let chunk_size = layout.stripe_size;
        let write_end = offset + data.len() as u64;

        // Check if write starts and ends on chunk boundaries
        let starts_on_boundary = offset.is_multiple_of(chunk_size);

        // Check if write covers at least one full chunk
        let mut covers_full_chunk = false;
        if starts_on_boundary {
            let first_chunk = (offset / chunk_size) as u32;
            let last_chunk = ((write_end - 1) / chunk_size) as u32;
            for chunk_idx in first_chunk..=last_chunk {
                let chunk_start = chunk_idx as u64 * chunk_size;
                let chunk_end = chunk_start + chunk_size;
                if offset <= chunk_start && write_end >= chunk_end {
                    covers_full_chunk = true;
                    break;
                }
            }
        }

        // Only write through if we're covering at least one full chunk
        // Partial writes are buffered and will be flushed later
        if covers_full_chunk {
            if let Err(e) = self.write_through(ino, &meta, logical_size, offset, data) {
                return reply.error(e);
            }
        } else {
            // Buffer the write
            if let Some(file_ref) = self.open_files.get_mut(&fh_val) {
                let mut wb = file_ref.write_buf.lock();
                wb.push((offset, data.to_vec()));
            }
        }

        if let Some(mut file_ref) = self.open_files.get_mut(&fh_val) {
            let file = file_ref.value_mut();
            file.tracked_size.store(logical_size, Ordering::Relaxed);
            file.meta.size = logical_size;
            if !pending && logical_size != meta.size {
                file.size_dirty = true;
            }
        }
        self.inodes.update_size(ino, logical_size);

        reply.written(data.len() as u32);
    }

    // -- flush: called on every close() of an fd --
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

    // -- fsync: ensure data is persisted to disk --
    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        // Same as flush — write all buffered data to OSTs
        match self.flush_writes(fh.0) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    // -- release: final close of a file --
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
        debug!("release: fh={fh_val}");
        // Flush any remaining writes
        let _ = self.flush_writes(fh_val);

        // Remove the file handle
        self.open_files.remove(&fh_val);
        reply.ok();
    }

    // -- link: create a hard link --
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
                RpcKind::Error(e) => {
                    if e.contains("not found") {
                        Err(Errno::ENOENT)
                    } else if e.contains("already exists") {
                        Err(Errno::EEXIST)
                    } else if e.contains("invalid argument") || e.contains("cannot hard-link") {
                        Err(Errno::EPERM)
                    } else {
                        Err(Errno::EIO)
                    }
                }
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

    // -- symlink: create a symbolic link --
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
                RpcKind::Error(e) => {
                    if e.contains("already exists") {
                        Err(Errno::EEXIST)
                    } else if e.contains("not found") {
                        Err(Errno::ENOENT)
                    } else if e.contains("not a directory") {
                        Err(Errno::ENOTDIR)
                    } else {
                        Err(Errno::EIO)
                    }
                }
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

    // -- readlink: read symbolic link target --
    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        let meta = match self.inodes.get_meta(ino.0) {
            Some(m) => m,
            None => return reply.error(Errno::ENOENT),
        };

        match meta.symlink_target {
            Some(ref target) => reply.data(target.as_bytes()),
            None => reply.error(Errno::EINVAL), // Not a symlink
        }
    }

    // -- copy_file_range: server-side copy between two open files --
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
            let tracked = file_ref.tracked_size.load(Ordering::Relaxed);
            if tracked > m.size {
                m.size = tracked;
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

        // Read source data
        let data = match self.read_data(&src_meta, offset_in, actual_len as u32) {
            Ok(d) => d,
            Err(e) => return reply.error(e),
        };

        // Write to destination
        let dest_file = match self.open_files.get(&fh_out.0) {
            Some(f) => f,
            None => return reply.error(Errno::EBADF),
        };

        if !dest_file.writable {
            return reply.error(Errno::EBADF);
        }

        {
            let mut wb = dest_file.write_buf.lock();
            wb.push((offset_out, data.clone()));
        }

        // Update tracked size
        let new_end = offset_out + data.len() as u64;
        loop {
            let current = dest_file.tracked_size.load(Ordering::Relaxed);
            if new_end <= current {
                break;
            }
            if dest_file
                .tracked_size
                .compare_exchange_weak(current, new_end, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }

        let _ = ino_out; // Used implicitly through fh_out
        reply.written(data.len() as u32);
    }

    // -- ioctl: catch-all for ioctls (return not supported) --
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

    // -- xattr operations: macOS Finder relies on these heavily --
    // We don't actually store xattrs, but we need to respond correctly
    // so Finder doesn't hang or error out.

    fn listxattr(&self, _req: &Request, _ino: INodeNo, _size: u32, reply: fuser::ReplyXattr) {
        // No extended attributes — return empty list
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
        // Attribute not found — NO_XATTR is the portable alias
        // (ENODATA on Linux, ENOATTR on macOS)
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
        // Silently accept (pretend we stored it) — this prevents Finder errors
        // when it tries to set com.apple.quarantine etc.
        reply.ok();
    }

    fn removexattr(&self, _req: &Request, _ino: INodeNo, _name: &OsStr, reply: ReplyEmpty) {
        // Silently succeed — same rationale as setxattr
        reply.ok();
    }

    // -- statfs: filesystem statistics --
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
            total_blocks, // blocks
            free_blocks,  // bfree
            avail_blocks, // bavail
            0,            // files (inodes — unknown)
            0,            // ffree
            bsize as u32, // bsize
            255,          // namelen
            bsize as u32, // frsize
        );
    }
}

// ---------------------------------------------------------------------------
// Mount entry point
// ---------------------------------------------------------------------------

/// Mount the Rustre filesystem at the given mountpoint.
pub fn mount(mgs_addr: &str, mountpoint: &str) -> Result<(), RustreError> {
    info!("Mounting Rustre filesystem at {mountpoint} (MGS: {mgs_addr})");

    let fs = RustreFs::new(mgs_addr)?;

    let mut config = Config::default();
    config.mount_options = vec![
        MountOption::FSName("rustre".to_string()),
        MountOption::AutoUnmount,
        MountOption::DefaultPermissions,
    ];
    config.acl = SessionACL::All;

    fuser::mount2(fs, mountpoint, &config).map_err(|e| {
        RustreError::Io(std::io::Error::new(
            e.kind(),
            format!("FUSE mount failed at {mountpoint}: {e}"),
        ))
    })?;

    Ok(())
}
