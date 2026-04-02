//! Core types for the FUSE mount implementation.
//!
//! Contains the inode cache, open-file state, and the `RustreFs` struct that
//! ties everything together.  All types use lock-free or fine-grained locking
//! to maximise FUSE callback throughput.

use crate::client::operations::{get_config, mds_addr};
use crate::error::RustreError;
use crate::rpc::{rpc_call, RpcKind};
use crate::types::{ClusterConfig, FileMeta};
use dashmap::DashMap;
use fuser::{FileType, INodeNo};
use parking_lot::{Mutex, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// TTL for cached attributes (FUSE kernel cache).
pub const ATTR_TTL: Duration = Duration::from_secs(1);
/// TTL for cached directory entries.
pub const ENTRY_TTL: Duration = Duration::from_secs(1);
/// Default stripe count for new files (0 = all OSTs).
pub const DEFAULT_STRIPE_COUNT: u32 = 0;
/// Default stripe size for new files (1 MiB).
pub const DEFAULT_STRIPE_SIZE: u64 = 1_048_576;
/// Default block size reported by statfs.
pub const BLOCK_SIZE: u32 = 4096;

// ---------------------------------------------------------------------------
// Inode cache — lock-free via DashMap
// ---------------------------------------------------------------------------

/// Thread-safe, lock-free inode cache.
///
/// Two `DashMap`s provide O(1) lookup in both directions:
///   - ino → `FileMeta`   (canonical metadata)
///   - path → ino         (reverse lookup for child_path → ino resolution)
///
/// All methods take `&self` — no outer lock required.
pub struct InodeMap {
    /// ino → cached metadata
    pub entries: DashMap<u64, FileMeta>,
    /// path → ino (for reverse lookups)
    pub path_to_ino: DashMap<String, u64>,
}

impl InodeMap {
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
            path_to_ino: DashMap::new(),
        }
    }

    /// Insert (or update) an inode in the cache.
    pub fn insert(&self, meta: FileMeta) {
        let ino = meta.ino;
        let path = meta.path.clone();
        self.path_to_ino.insert(path, ino);
        self.entries.insert(ino, meta);
    }

    /// Get a clone of the metadata for the given ino.
    pub fn get_meta(&self, ino: u64) -> Option<FileMeta> {
        self.entries.get(&ino).map(|e| e.value().clone())
    }

    /// Get the cached path for the given ino.
    pub fn get_path(&self, ino: u64) -> Option<String> {
        self.entries.get(&ino).map(|e| e.value().path.clone())
    }

    /// Update only the size and mtime of a cached inode.
    pub fn update_size(&self, ino: u64, size: u64) {
        if let Some(mut e) = self.entries.get_mut(&ino) {
            e.size = size;
            e.mtime = FileMeta::now_secs();
        }
    }

    /// Update the pending flag of a cached inode.
    #[allow(dead_code)]
    pub fn update_pending(&self, ino: u64, pending: bool) {
        if let Some(mut e) = self.entries.get_mut(&ino) {
            e.pending = pending;
        }
    }

    /// Remove an inode (and its path mapping) from the cache.
    pub fn remove(&self, ino: u64) {
        if let Some((_, meta)) = self.entries.remove(&ino) {
            self.path_to_ino.remove(&meta.path);
        }
    }

    /// Rename: update cached path mappings atomically (best-effort).
    pub fn rename(&self, ino: u64, old_path: &str, new_path: &str, new_name: &str, new_parent: u64) {
        self.path_to_ino.remove(old_path);
        self.path_to_ino.insert(new_path.to_string(), ino);
        if let Some(mut entry) = self.entries.get_mut(&ino) {
            entry.path = new_path.to_string();
            entry.name = new_name.to_string();
            entry.parent_ino = new_parent;
            entry.mtime = FileMeta::now_secs();
        }
    }
}

// ---------------------------------------------------------------------------
// Per-inode open state (tracks all fh's for a given ino)
// ---------------------------------------------------------------------------

/// Tracks the state of all open file descriptors for a single inode.
///
/// This is keyed by *inode* (not by fh) so that flush/fsync can consistently
/// synchronise across all open handles — required for build tools that open
/// the same file from multiple threads/processes.
pub struct InodeState {
    /// Reference count: how many fh's are open for this inode.
    pub refcount: AtomicU64,
    /// Whether any handle has outstanding writes that haven't been synced to MDS.
    pub size_dirty: Mutex<bool>,
    /// The most recent tracked file size across all handles.
    pub tracked_size: AtomicU64,
    /// Whether this inode has been unlinked while open (deferred deletion).
    pub detached: Mutex<bool>,
    /// The path at the time of detachment (for OSS cleanup).
    pub detached_meta: Mutex<Option<FileMeta>>,
}

impl InodeState {
    pub fn new(initial_size: u64) -> Self {
        Self {
            refcount: AtomicU64::new(0),
            size_dirty: Mutex::new(false),
            tracked_size: AtomicU64::new(initial_size),
            detached: Mutex::new(false),
            detached_meta: Mutex::new(None),
        }
    }

    pub fn acquire(&self) {
        self.refcount.fetch_add(1, Ordering::Relaxed);
    }

    pub fn release(&self) -> u64 {
        self.refcount.fetch_sub(1, Ordering::AcqRel) - 1
    }

    /// Atomically update tracked_size to `max(current, new_size)`.
    pub fn update_size(&self, new_size: u64) {
        loop {
            let current = self.tracked_size.load(Ordering::Relaxed);
            if new_size <= current {
                break;
            }
            if self
                .tracked_size
                .compare_exchange_weak(current, new_size, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Open file handle — per-fh state
// ---------------------------------------------------------------------------

/// Per file-handle state.
///
/// Each `open()` / `create()` returns a unique fh number mapped to one of
/// these.  Multiple fh's may reference the same `InodeState`.
pub struct OpenFile {
    /// The inode this handle refers to.
    pub ino: u64,
    /// Snapshot of metadata at open time (path, layout, etc.).
    pub meta: FileMeta,
    /// Per-handle write buffer for partial-chunk writes.
    /// Flushed on flush/fsync/release.
    pub write_buf: Mutex<Vec<(u64, Vec<u8>)>>,
    /// Whether this handle was opened for writing.
    pub writable: bool,
}

// ---------------------------------------------------------------------------
// Open directory snapshot
// ---------------------------------------------------------------------------

/// Type alias for directory entry snapshots taken at opendir time.
pub type DirSnapshot = Vec<(INodeNo, FileType, String)>;

// ---------------------------------------------------------------------------
// RustreFs — the FUSE filesystem
// ---------------------------------------------------------------------------

/// The main FUSE filesystem struct.
///
/// Holds the tokio runtime, cluster config, inode cache, and open-file maps.
/// All FUSE callbacks are synchronous; async operations use `rt.block_on()`.
pub struct RustreFs {
    /// Tokio runtime for async operations.
    pub rt: tokio::runtime::Runtime,
    /// Cached cluster configuration (refreshed periodically).
    pub config: Arc<RwLock<ClusterConfig>>,
    /// Inode cache (lock-free DashMap).
    pub inodes: Arc<InodeMap>,
    /// Per-inode open state (ref counting + tracked size).
    pub inode_state: Arc<DashMap<u64, Arc<InodeState>>>,
    /// Open file handles: fh → OpenFile.
    pub open_files: Arc<DashMap<u64, OpenFile>>,
    /// Open directory handles: fh → snapshot at opendir time.
    pub open_dirs: Arc<DashMap<u64, DirSnapshot>>,
    /// Next file handle number.
    pub next_fh: AtomicU64,
}

impl RustreFs {
    /// Create a new RustreFs instance connected to the given MGS.
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

        // FUSE always uses ino=1 for root.
        let mut root = root_meta;
        root.ino = 1;
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
            inode_state: Arc::new(DashMap::new()),
            open_files: Arc::new(DashMap::new()),
            open_dirs: Arc::new(DashMap::new()),
            next_fh: AtomicU64::new(1),
        })
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Snapshot cluster config (clone under brief read lock, then drop lock).
    pub fn snap_config(&self) -> ClusterConfig {
        self.config.read().clone()
    }

    /// Pick a random MDS address from snapshot.
    pub fn pick_mds(&self) -> Result<String, fuser::Errno> {
        let cfg = self.snap_config();
        mds_addr(&cfg).map_err(super::helpers::rustre_err_to_errno)
    }

    /// Stat a path via MDS, returning FileMeta on success.
    pub fn stat_path(&self, path: &str) -> Result<FileMeta, fuser::Errno> {
        let mds = self.pick_mds()?;
        self.rt.block_on(async {
            let reply = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                rpc_call(&mds, RpcKind::Stat(path.to_string())),
            )
            .await;
            match reply {
                Ok(Ok(r)) => match r.kind {
                    RpcKind::MetaReply(m) => Ok(m),
                    RpcKind::Error(e) => {
                        if e.contains("not found") {
                            Err(fuser::Errno::ENOENT)
                        } else {
                            Err(fuser::Errno::EIO)
                        }
                    }
                    _ => Err(fuser::Errno::EIO),
                },
                Ok(Err(_)) => Err(fuser::Errno::EIO),
                Err(_timeout) => Err(fuser::Errno::EIO),
            }
        })
    }

    /// Build child path from parent ino + child name.
    pub fn child_path(&self, parent: u64, name: &std::ffi::OsStr) -> Result<String, fuser::Errno> {
        let name_str = name.to_str().ok_or(fuser::Errno::EINVAL)?;
        let parent_path = self.inodes.get_path(parent).ok_or(fuser::Errno::ENOENT)?;
        if parent_path == "/" {
            Ok(format!("/{name_str}"))
        } else {
            Ok(format!("{parent_path}/{name_str}"))
        }
    }

    /// Get or create the per-inode state for the given ino.
    pub fn get_or_create_inode_state(&self, ino: u64, initial_size: u64) -> Arc<InodeState> {
        self.inode_state
            .entry(ino)
            .or_insert_with(|| Arc::new(InodeState::new(initial_size)))
            .value()
            .clone()
    }

    /// Allocate a new file-handle number.
    pub fn alloc_fh(&self) -> u64 {
        self.next_fh.fetch_add(1, Ordering::Relaxed)
    }

    /// Sync the tracked file size to MDS using SetSizeByIno (preferred) with
    /// fallback to SetSize (path-based) for compatibility.
    pub fn sync_size_to_mds(&self, ino: u64, path: &str, size: u64) -> Result<(), fuser::Errno> {
        let mds = self.pick_mds()?;
        tracing::debug!("sync_size_to_mds: ino={ino:#x} path={path} size={size} mds={mds}");
        self.rt.block_on(async {
            // Try inode-based SetSize first, with a 5-second timeout
            tracing::debug!("sync_size_to_mds: sending SetSizeByIno ino={ino:#x} size={size}");
            let reply = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                rpc_call(&mds, RpcKind::SetSizeByIno { ino, size }),
            )
            .await;
            tracing::debug!("sync_size_to_mds: SetSizeByIno reply={reply:?}");

            match reply {
                Ok(Ok(r)) => match r.kind {
                    RpcKind::Ok => return Ok(()),
                    RpcKind::Error(ref e) if e.contains("unsupported") => {
                        tracing::debug!("sync_size_to_mds: unsupported, falling back");
                    }
                    RpcKind::Error(e) => {
                        tracing::error!("SetSizeByIno failed: {e}");
                        return Err(fuser::Errno::EIO);
                    }
                    _ => return Err(fuser::Errno::EIO),
                },
                Ok(Err(e)) => {
                    tracing::debug!("sync_size_to_mds: SetSizeByIno error: {e}, falling back");
                }
                Err(_timeout) => {
                    tracing::warn!("sync_size_to_mds: SetSizeByIno timed out, falling back");
                }
            }

            // Fallback: path-based SetSize with timeout
            tracing::debug!("sync_size_to_mds: fallback to SetSize path={path} size={size}");
            let rpc_reply = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                rpc_call(&mds, RpcKind::SetSize { path: path.to_string(), size }),
            )
            .await;

            match rpc_reply {
                Ok(Ok(r)) => match r.kind {
                    RpcKind::Ok => Ok(()),
                    RpcKind::Error(e) => {
                        tracing::error!("SetSize fallback failed: {e}");
                        Err(fuser::Errno::EIO)
                    }
                    _ => Err(fuser::Errno::EIO),
                },
                Ok(Err(e)) => {
                    tracing::error!("SetSize fallback RPC error: {e}");
                    Err(fuser::Errno::EIO)
                }
                Err(_timeout) => {
                    tracing::error!("SetSize fallback timed out");
                    Err(fuser::Errno::EIO)
                }
            }
        })
    }
}
