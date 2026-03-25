//! FUSE mount — makes Rustre mountable as a POSIX filesystem on macOS/Linux.
//!
//! Architecture:
//!   - `RustreFs` implements `fuser::Filesystem` (async via a dedicated tokio runtime)
//!   - Inode numbers from Rustre MDS are used directly as FUSE inode numbers
//!   - `InodeMap` caches ino→path and ino→FileMeta for fast lookups
//!   - File handles track open files with their stripe layout
//!   - Write buffering aggregates small writes into stripe-sized chunks
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
use fuser::{
    BsdFileFlags, Config, Errno, FileHandle, FopenFlags, Generation, INodeNo, LockOwner, OpenFlags,
    SessionACL, TimeOrNow, WriteFlags,
};
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request,
};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tracing::{error, info, warn};

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
        perm: if meta.is_dir { 0o755 } else { 0o644 },
        nlink: if meta.is_dir { 2 } else { 1 },
        uid: unsafe { libc::getuid() },
        gid: unsafe { libc::getgid() },
        rdev: 0,
        blksize: BLOCK_SIZE,
        flags: 0,
    }
}

// ---------------------------------------------------------------------------
// Inode cache: ino → (path, FileMeta)
// ---------------------------------------------------------------------------

struct InodeEntry {
    meta: FileMeta,
}

struct InodeMap {
    /// ino → cached metadata
    entries: HashMap<u64, InodeEntry>,
    /// path → ino (for reverse lookups, e.g. parent_ino + name → child ino)
    path_to_ino: HashMap<String, u64>,
}

impl InodeMap {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            path_to_ino: HashMap::new(),
        }
    }

    fn insert(&mut self, meta: FileMeta) {
        let ino = meta.ino;
        let path = meta.path.clone();
        self.path_to_ino.insert(path, ino);
        self.entries.insert(ino, InodeEntry { meta });
    }

    fn get(&self, ino: u64) -> Option<&FileMeta> {
        self.entries.get(&ino).map(|e| &e.meta)
    }

    fn get_path(&self, ino: u64) -> Option<&str> {
        self.entries.get(&ino).map(|e| e.meta.path.as_str())
    }

    fn remove(&mut self, ino: u64) {
        if let Some(entry) = self.entries.remove(&ino) {
            self.path_to_ino.remove(&entry.meta.path);
        }
    }
}

// ---------------------------------------------------------------------------
// Open file handle — tracks open files for reads/writes
// ---------------------------------------------------------------------------

struct OpenFile {
    ino: u64,
    meta: FileMeta,
    /// Write buffer: offset → data. Flushed on flush/release.
    write_buf: HashMap<u64, Vec<u8>>,
    /// Whether this file was opened for writing
    writable: bool,
    /// For new files created via create(): true until committed
    pending: bool,
}

// ---------------------------------------------------------------------------
// RustreFs — the FUSE filesystem implementation
// ---------------------------------------------------------------------------

pub struct RustreFs {
    /// Tokio runtime for async operations
    rt: tokio::runtime::Runtime,
    /// Cached cluster configuration (refreshed periodically)
    config: Arc<RwLock<ClusterConfig>>,
    /// Inode cache
    inodes: Arc<RwLock<InodeMap>>,
    /// Open file handles: fh → OpenFile
    open_files: Arc<RwLock<HashMap<u64, OpenFile>>>,
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
        let mut inode_map = InodeMap::new();

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
                        if let Ok(mut c) = config_clone.write() {
                            *c = cfg;
                        }
                    }
                    Err(e) => warn!("config refresh failed: {e}"),
                }
            }
        });

        Ok(Self {
            rt,
            config,
            inodes: Arc::new(RwLock::new(inode_map)),
            open_files: Arc::new(RwLock::new(HashMap::new())),
            next_fh: AtomicU64::new(1),
        })
    }

    // -----------------------------------------------------------------------
    // Internal helpers — run async operations on the embedded runtime
    // -----------------------------------------------------------------------

    /// Get a random MDS address from cached config.
    fn pick_mds(&self) -> Result<String, Errno> {
        let cfg = self.config.read().map_err(|_| Errno::EIO)?;
        mds_addr(&cfg).map_err(rustre_err_to_errno)
    }

    /// Get cluster config snapshot.
    fn get_cached_config(&self) -> Result<ClusterConfig, Errno> {
        self.config
            .read()
            .map(|c| c.clone())
            .map_err(|_| Errno::EIO)
    }

    /// Stat a path via MDS, returning FileMeta on success.
    fn stat_path(&self, path: &str) -> Result<FileMeta, Errno> {
        self.rt.block_on(async {
            let mds = self.pick_mds()?;
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
        let inodes = self.inodes.read().map_err(|_| Errno::EIO)?;
        let parent_path = inodes.get_path(parent).ok_or(Errno::ENOENT)?;
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
        let config = self.get_cached_config()?;

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

                // Fetch the whole chunk from OST
                let object_id = StripeLayout::object_id(meta.ino, chunk_idx);
                let primary_ost = layout.ost_for_chunk(chunk_idx);
                let addr = ost_addr(&config, primary_ost).map_err(rustre_err_to_errno)?;

                let chunk_data = read_chunk_from_ost(&addr, &object_id).await?;

                // Copy the relevant portion into result
                if chunk_data.len() >= read_end_in_chunk {
                    let src = &chunk_data[read_start_in_chunk..read_end_in_chunk];
                    let dst = &mut result[buf_start..buf_start + src.len()];
                    dst.copy_from_slice(src);
                }
            }
            Ok(result)
        })
    }

    /// Flush write buffers for an open file — performs the two-phase commit.
    fn flush_writes(&self, fh: u64) -> Result<(), Errno> {
        // Take the write buffer out
        let (ino, meta, write_buf, pending) = {
            let mut files = self.open_files.write().map_err(|_| Errno::EIO)?;
            let file = files.get_mut(&fh).ok_or(Errno::EBADF)?;
            if file.write_buf.is_empty() && !file.pending {
                return Ok(());
            }
            let buf = std::mem::take(&mut file.write_buf);
            (file.ino, file.meta.clone(), buf, file.pending)
        };

        if write_buf.is_empty() {
            // Still need to commit if pending
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
                // Mark as no longer pending
                if let Ok(mut files) = self.open_files.write() {
                    if let Some(file) = files.get_mut(&fh) {
                        file.pending = false;
                    }
                }
            }
            return Ok(());
        }

        let layout = meta.layout.as_ref().ok_or(Errno::EIO)?;
        let config = self.get_cached_config()?;
        let chunk_size = layout.stripe_size as usize;

        // Group writes by chunk index
        let mut chunks: HashMap<u32, Vec<u8>> = HashMap::new();
        for (&off, data) in &write_buf {
            let start_chunk = (off / chunk_size as u64) as u32;
            let end_off = off + data.len() as u64;
            let end_chunk = if end_off == 0 {
                0
            } else {
                ((end_off - 1) / chunk_size as u64) as u32
            };

            for ci in start_chunk..=end_chunk {
                let chunk_base = ci as u64 * chunk_size as u64;
                let chunk = chunks.entry(ci).or_insert_with(|| vec![0u8; chunk_size]);

                let data_start_in_chunk = if off > chunk_base {
                    (off - chunk_base) as usize
                } else {
                    0
                };
                let data_off = if chunk_base > off {
                    (chunk_base - off) as usize
                } else {
                    0
                };
                let copy_len =
                    std::cmp::min(data.len() - data_off, chunk_size - data_start_in_chunk);

                chunk[data_start_in_chunk..data_start_in_chunk + copy_len]
                    .copy_from_slice(&data[data_off..data_off + copy_len]);
            }
        }

        // Compute actual file size from writes
        let mut max_offset: u64 = meta.size;
        for (&off, data) in &write_buf {
            let end = off + data.len() as u64;
            if end > max_offset {
                max_offset = end;
            }
        }

        // Write each chunk to the appropriate OST
        self.rt.block_on(async {
            let mut write_handles = Vec::new();

            for (chunk_idx, data) in &chunks {
                let object_id = StripeLayout::object_id(ino, *chunk_idx);
                let ost_index = layout.ost_for_chunk(*chunk_idx);
                let addr = ost_addr(&config, ost_index).map_err(rustre_err_to_errno)?;

                // Trim trailing zeros for the last chunk
                let actual_size = if (*chunk_idx as u64 + 1) * chunk_size as u64 > max_offset {
                    (max_offset - *chunk_idx as u64 * chunk_size as u64) as usize
                } else {
                    chunk_size
                };
                let data_to_write = data[..actual_size].to_vec();
                let addr = addr.clone();

                write_handles.push(tokio::spawn(async move {
                    write_chunk_to_ost(&addr, &object_id, &data_to_write).await
                }));
            }

            // Wait for all writes
            for handle in write_handles {
                handle
                    .await
                    .map_err(|_| Errno::EIO)?
                    .map_err(|_| Errno::EIO)?;
            }

            // Commit: set size and make visible
            let mds = self.pick_mds()?;
            let reply = rpc_call(
                &mds,
                RpcKind::CommitCreate {
                    ino,
                    size: max_offset,
                },
            )
            .await
            .map_err(rustre_err_to_errno)?;

            match reply.kind {
                RpcKind::Ok => {}
                RpcKind::Error(e) => {
                    error!("commit failed: {e}");
                    return Err(Errno::EIO);
                }
                _ => return Err(Errno::EIO),
            }

            Ok(())
        })?;

        // Update cached metadata
        {
            let mut files = self.open_files.write().map_err(|_| Errno::EIO)?;
            if let Some(file) = files.get_mut(&fh) {
                file.meta.size = max_offset;
                file.meta.pending = false;
                file.pending = false;
            }
        }
        {
            let mut inodes = self.inodes.write().map_err(|_| Errno::EIO)?;
            if let Some(entry) = inodes.entries.get_mut(&ino) {
                entry.meta.size = max_offset;
                entry.meta.pending = false;
            }
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
    // -- lookup: resolve parent + name → child attributes --
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let path = match self.child_path(parent.0, name) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };

        match self.stat_path(&path) {
            Ok(meta) => {
                let attr = meta_to_attr(&meta);
                // Cache the inode
                if let Ok(mut inodes) = self.inodes.write() {
                    inodes.insert(meta);
                }
                reply.entry(&ENTRY_TTL, &attr, Generation(0));
            }
            Err(e) => reply.error(e),
        }
    }

    // -- getattr: get file attributes by ino --
    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        // Try cache first
        {
            let inodes = match self.inodes.read() {
                Ok(i) => i,
                Err(_) => return reply.error(Errno::EIO),
            };
            if let Some(meta) = inodes.get(ino.0) {
                return reply.attr(&ATTR_TTL, &meta_to_attr(meta));
            }
        }

        // Cache miss — need to ask MDS, but we need the path
        // For unknown inodes, return ENOENT (they'll get cached via lookup)
        reply.error(Errno::ENOENT);
    }

    // -- setattr: handle truncate (size changes) --
    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let ino_val = ino.0;

        // Handle truncate
        if let Some(new_size) = size {
            let path = {
                let inodes = match self.inodes.read() {
                    Ok(i) => i,
                    Err(_) => return reply.error(Errno::EIO),
                };
                match inodes.get_path(ino_val) {
                    Some(p) => p.to_string(),
                    None => return reply.error(Errno::ENOENT),
                }
            };

            let result = self.rt.block_on(async {
                let mds = self.pick_mds()?;
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

            // Update cache
            if let Ok(mut inodes) = self.inodes.write() {
                if let Some(entry) = inodes.entries.get_mut(&ino_val) {
                    entry.meta.size = new_size;
                }
            }
        }

        // Return current attributes
        let inodes = match self.inodes.read() {
            Ok(i) => i,
            Err(_) => return reply.error(Errno::EIO),
        };
        match inodes.get(ino_val) {
            Some(meta) => reply.attr(&ATTR_TTL, &meta_to_attr(meta)),
            None => reply.error(Errno::ENOENT),
        }
    }

    // -- readdir: list directory contents --
    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let ino_val = ino.0;
        let path = {
            let inodes = match self.inodes.read() {
                Ok(i) => i,
                Err(_) => return reply.error(Errno::EIO),
            };
            match inodes.get_path(ino_val) {
                Some(p) => p.to_string(),
                None => return reply.error(Errno::ENOENT),
            }
        };

        let entries = self.rt.block_on(async {
            let mds = self.pick_mds()?;
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

        // Build directory listing: . and .. first, then children
        let parent_ino = {
            let inodes = self.inodes.read().unwrap_or_else(|e| e.into_inner());
            inodes.get(ino_val).map(|m| m.parent_ino).unwrap_or(1)
        };

        let mut all_entries: Vec<(INodeNo, FileType, String)> = Vec::new();
        all_entries.push((INodeNo(ino_val), FileType::Directory, ".".to_string()));
        all_entries.push((
            INodeNo(if parent_ino == 0 { 1 } else { parent_ino }),
            FileType::Directory,
            "..".to_string(),
        ));

        // Cache children and build entries
        {
            let mut inodes = self.inodes.write().unwrap_or_else(|e| e.into_inner());
            for child in &entries {
                inodes.insert(child.clone());
            }
        }

        for entry in &entries {
            let kind = if entry.is_dir {
                FileType::Directory
            } else {
                FileType::RegularFile
            };
            all_entries.push((INodeNo(entry.ino), kind, entry.name.clone()));
        }

        // Apply offset and fill reply
        for (i, (entry_ino, kind, name)) in all_entries.iter().enumerate().skip(offset as usize) {
            // offset is 1-based for non-zero entries
            let next_offset = (i + 1) as u64;
            if reply.add(*entry_ino, next_offset, *kind, name) {
                break; // buffer full
            }
        }

        reply.ok();
    }

    // -- mkdir: create directory --
    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let path = match self.child_path(parent.0, name) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };

        let result = self.rt.block_on(async {
            let mds = self.pick_mds()?;
            let rpc_reply = rpc_call(&mds, RpcKind::Mkdir(path.clone()))
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
                if let Ok(mut inodes) = self.inodes.write() {
                    inodes.insert(meta);
                }
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

        // Get file metadata first (need ino for OSS cleanup)
        let meta = match self.stat_path(&path) {
            Ok(m) => m,
            Err(e) => return reply.error(e),
        };

        let ino = meta.ino;
        let has_objects = meta.layout.is_some();
        let config = match self.get_cached_config() {
            Ok(c) => c,
            Err(e) => return reply.error(e),
        };

        // MDS unlink first (same protocol as client/rm.rs)
        let result = self.rt.block_on(async {
            let mds = self.pick_mds()?;
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

        // Best-effort OSS cleanup (same as client/rm.rs)
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

        // Remove from cache
        if let Ok(mut inodes) = self.inodes.write() {
            inodes.remove(ino);
        }

        reply.ok();
    }

    // -- rmdir: remove a directory --
    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let path = match self.child_path(parent.0, name) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };

        let result = self.rt.block_on(async {
            let mds = self.pick_mds()?;
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
                // Remove from cache
                let ino = {
                    let inodes = self.inodes.read().unwrap_or_else(|e| e.into_inner());
                    inodes.path_to_ino.get(&path).copied()
                };
                if let Some(ino) = ino {
                    if let Ok(mut inodes) = self.inodes.write() {
                        inodes.remove(ino);
                    }
                }
                reply.ok();
            }
            Err(e) => reply.error(e),
        }
    }

    // -- open: open a file, return a file handle --
    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let ino_val = ino.0;
        let meta = {
            let inodes = match self.inodes.read() {
                Ok(i) => i,
                Err(_) => return reply.error(Errno::EIO),
            };
            match inodes.get(ino_val) {
                Some(m) => m.clone(),
                None => {
                    // Try to fetch from MDS if not cached
                    drop(inodes);
                    // We don't have the path, so we can't stat
                    return reply.error(Errno::ENOENT);
                }
            }
        };

        let raw_flags = flags.0;
        let writable = (raw_flags & libc::O_WRONLY != 0) || (raw_flags & libc::O_RDWR != 0);
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);

        let open_file = OpenFile {
            ino: ino_val,
            meta,
            write_buf: HashMap::new(),
            writable,
            pending: false,
        };

        if let Ok(mut files) = self.open_files.write() {
            files.insert(fh, open_file);
        }

        reply.opened(FileHandle(fh), FopenFlags::empty());
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

        // Get metadata from open file handle or cache
        let meta = {
            let files = match self.open_files.read() {
                Ok(f) => f,
                Err(_) => return reply.error(Errno::EIO),
            };
            if let Some(file) = files.get(&fh_val) {
                file.meta.clone()
            } else {
                drop(files);
                let inodes = match self.inodes.read() {
                    Ok(i) => i,
                    Err(_) => return reply.error(Errno::EIO),
                };
                match inodes.get(ino_val) {
                    Some(m) => m.clone(),
                    None => return reply.error(Errno::ENOENT),
                }
            }
        };

        if meta.is_dir {
            return reply.error(Errno::EISDIR);
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
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let path = match self.child_path(parent.0, name) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };

        // Create pending file on MDS (same as client/put.rs phase 1)
        let meta = self.rt.block_on(async {
            let mds = self.pick_mds()?;
            let rpc_reply = rpc_call(
                &mds,
                RpcKind::Create(CreateReq {
                    path: path.clone(),
                    stripe_count: DEFAULT_STRIPE_COUNT,
                    stripe_size: DEFAULT_STRIPE_SIZE,
                    replica_count: 1,
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

        let attr = meta_to_attr(&meta);
        let ino = meta.ino;
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);

        // Cache the inode
        if let Ok(mut inodes) = self.inodes.write() {
            inodes.insert(meta.clone());
        }

        // Create open file handle
        let open_file = OpenFile {
            ino,
            meta,
            write_buf: HashMap::new(),
            writable: true,
            pending: true, // Will be committed on flush/release
        };

        if let Ok(mut files) = self.open_files.write() {
            files.insert(fh, open_file);
        }

        reply.created(
            &ENTRY_TTL,
            &attr,
            Generation(0),
            FileHandle(fh),
            FopenFlags::empty(),
        );
    }

    // -- write: buffer write data --
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
        let mut files = match self.open_files.write() {
            Ok(f) => f,
            Err(_) => return reply.error(Errno::EIO),
        };
        let file = match files.get_mut(&fh_val) {
            Some(f) => f,
            None => return reply.error(Errno::EBADF),
        };

        if !file.writable {
            return reply.error(Errno::EBADF);
        }

        // Buffer the write data
        file.write_buf.insert(offset, data.to_vec());

        // Update file size in our tracking
        let new_end = offset + data.len() as u64;
        if new_end > file.meta.size {
            file.meta.size = new_end;
        }

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
        // Flush any remaining writes
        let _ = self.flush_writes(fh_val);

        // Remove the file handle
        if let Ok(mut files) = self.open_files.write() {
            files.remove(&fh_val);
        }

        reply.ok();
    }

    // -- statfs: filesystem statistics --
    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        let config = match self.get_cached_config() {
            Ok(c) => c,
            Err(e) => return reply.error(e),
        };

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
