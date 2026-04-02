//! File system operations for MDS

use crate::error::{Result, RustreError};
use crate::mds::path_utils;
use crate::rpc::{make_reply, rpc_call, RpcKind, RpcMessage};
use crate::storage::FdbMdsStore;
use crate::types::{ClusterConfig, CreateReq, FileMeta, StripeLayout, DEFAULT_STRIPE_SIZE};
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, info};

/// Default number of inodes to request per batch from MGS.
const INODE_RANGE_BATCH: u64 = 10_000;

/// Lock-free local inode allocator.
///
/// Each MDS instance holds a range `[next, end)` obtained from MGS.
/// Allocation is a single atomic fetch_add — zero contention, zero FDB traffic.
/// When the range is exhausted, we request a new batch from MGS.
///
/// No return/reclaim protocol: u64 inode space (~1.8×10¹⁹) is effectively
/// infinite. Leaked ranges from crashes or shutdowns are harmless.
pub struct InodeAllocator {
    /// Next inode to allocate (atomic for lock-free fast path).
    next: AtomicU64,
    /// Exclusive upper bound of current range.
    end: AtomicU64,
    /// Serialize range refill requests (only one refill at a time).
    refill_lock: Mutex<()>,
    /// MGS address for requesting new ranges.
    mgs_addr: String,
}

impl InodeAllocator {
    /// Create allocator with an initial range obtained from MGS.
    pub fn new(start: u64, end: u64, mgs_addr: String) -> Self {
        Self {
            next: AtomicU64::new(start),
            end: AtomicU64::new(end),
            refill_lock: Mutex::new(()),
            mgs_addr,
        }
    }

    /// Allocate a single inode number.
    ///
    /// Fast path: atomic increment, no I/O.
    /// Slow path (range exhausted): RPC to MGS for a new batch.
    pub async fn alloc(&self) -> Result<u64> {
        // Fast path: try to claim one from current range
        let ino = self.next.fetch_add(1, Ordering::Relaxed);
        if ino < self.end.load(Ordering::Relaxed) {
            return Ok(ino);
        }

        // Slow path: range exhausted, need to refill
        // Undo the speculative increment so other threads also hit the slow path
        self.next.fetch_sub(1, Ordering::Relaxed);

        // Serialize refill requests
        let _guard = self.refill_lock.lock().await;

        // Double-check: another thread may have refilled while we waited
        let ino = self.next.fetch_add(1, Ordering::Relaxed);
        if ino < self.end.load(Ordering::Relaxed) {
            return Ok(ino);
        }
        self.next.fetch_sub(1, Ordering::Relaxed);

        // Request new range from MGS
        let reply = rpc_call(
            &self.mgs_addr,
            RpcKind::AllocInodeRange {
                count: INODE_RANGE_BATCH,
            },
        )
        .await?;

        match reply.kind {
            RpcKind::InodeRangeReply { start, end } => {
                info!(
                    "MDS: refilled inode range [{start}, {end}) ({} inodes)",
                    end - start
                );
                self.next.store(start + 1, Ordering::Relaxed);
                self.end.store(end, Ordering::Relaxed);
                Ok(start)
            }
            RpcKind::Error(e) => Err(RustreError::Net(format!("alloc inode range: {e}"))),
            _ => Err(RustreError::Net(
                "unexpected reply for AllocInodeRange".into(),
            )),
        }
    }
}

/// Request an initial inode range from MGS.
pub async fn alloc_initial_inode_range(mgs_addr: &str) -> Result<(u64, u64)> {
    let reply = rpc_call(
        mgs_addr,
        RpcKind::AllocInodeRange {
            count: INODE_RANGE_BATCH,
        },
    )
    .await?;
    match reply.kind {
        RpcKind::InodeRangeReply { start, end } => {
            info!(
                "MDS: initial inode range [{start}, {end}) ({} inodes)",
                end - start
            );
            Ok((start, end))
        }
        RpcKind::Error(e) => Err(RustreError::Net(format!("alloc inode range: {e}"))),
        _ => Err(RustreError::Net(
            "unexpected reply for AllocInodeRange".into(),
        )),
    }
}

/// MDS runtime state - split to reduce lock contention
/// Only cluster_config needs synchronization, other fields are thread-safe
pub struct MdsState {
    pub store: Arc<FdbMdsStore>,
    pub cluster_config: RwLock<ClusterConfig>,
    pub mgs_addr: String,
    pub ino_alloc: Arc<InodeAllocator>,
}

/// Fetch cluster config from MGS
pub async fn fetch_config(mgs_addr: &str) -> Result<ClusterConfig> {
    let reply = rpc_call(mgs_addr, RpcKind::GetConfig).await?;
    match reply.kind {
        RpcKind::ConfigReply(cfg) => Ok(cfg),
        RpcKind::Error(e) => Err(RustreError::Net(e)),
        _ => Err(RustreError::Net("unexpected reply from MGS".into())),
    }
}

/// Register MDS with MGS
pub async fn register_with_mgs(mgs_addr: &str, listen: &str) -> Result<()> {
    // Convert "0.0.0.0:port" to "127.0.0.1:port" for local registration
    let addr = if listen.starts_with("0.0.0.0") {
        listen.replace("0.0.0.0", "127.0.0.1")
    } else {
        listen.to_string()
    };
    let reply = rpc_call(
        mgs_addr,
        RpcKind::RegisterMds(crate::types::MdsInfo {
            address: addr.clone(),
        }),
    )
    .await?;
    match reply.kind {
        RpcKind::Ok => {
            info!("MDS registered with MGS as {addr}");
            Ok(())
        }
        RpcKind::Error(e) => Err(RustreError::Net(e)),
        _ => Err(RustreError::Net("unexpected reply from MGS".into())),
    }
}

/// Handle lookup operation.
/// Pending files (write-intent not yet committed) are invisible to lookups.
pub async fn handle_lookup(req_id: u64, path: &str, state: &MdsState) -> Result<RpcMessage> {
    let path = path_utils::normalize_path(path);

    let ino = state
        .store
        .resolve_path(&path)
        .await?
        .ok_or_else(|| RustreError::NotFound(path.clone()))?;

    let meta = state
        .store
        .get_inode(ino)
        .await?
        .ok_or_else(|| RustreError::NotFound(path.clone()))?;

    // Pending files are not visible to external lookups/stat
    if meta.pending {
        return Err(RustreError::NotFound(path));
    }

    Ok(make_reply(req_id, RpcKind::MetaReply(meta)))
}

/// Handle create operation
pub async fn handle_create(req_id: u64, req: CreateReq, state: &MdsState) -> Result<RpcMessage> {
    let path = path_utils::normalize_path(&req.path);
    let parent = path_utils::parent_path(&path);
    let name = path_utils::basename(&path);

    // Check parent exists and is a directory
    let parent_ino = state
        .store
        .resolve_path(&parent)
        .await?
        .ok_or_else(|| RustreError::NotFound(format!("parent directory: {parent}")))?;

    let parent_meta = state
        .store
        .get_inode(parent_ino)
        .await?
        .ok_or_else(|| RustreError::NotFound(parent.clone()))?;

    if !parent_meta.is_dir {
        return Err(RustreError::NotADirectory(parent));
    }

    // Check if already exists
    if (state.store.resolve_path(&path).await?).is_some() {
        return Err(RustreError::AlreadyExists(path));
    }

    // Fetch latest OST info from MGS to ensure we have up-to-date available OSTs
    // This is critical because OSTs may go down and the cached config could be stale
    let latest_config = fetch_config(&state.mgs_addr).await?;

    // Update the cached cluster_config with the latest info
    {
        let mut config_write = state.cluster_config.write().await;
        *config_write = latest_config.clone();
    }

    // Get read access to config for stripe layout computation
    let config_read = state.cluster_config.read().await;

    // Compute stripe layout using actual available OST indices from latest config
    // Note: ost_list may have gaps (e.g., [ost-0, ost-2, ost-3] if ost-1 is down)
    // We must use the actual ost_index values, not positions in the list
    let available_osts: Vec<u32> = config_read
        .ost_list
        .iter()
        .map(|ost| ost.ost_index)
        .collect();
    let ost_count = available_osts.len() as u32;
    if ost_count == 0 {
        return Err(RustreError::NoOstAvailable);
    }
    let stripe_count = if req.stripe_count == 0 || req.stripe_count > ost_count {
        ost_count
    } else {
        req.stripe_count
    };
    let stripe_size = if req.stripe_size == 0 {
        DEFAULT_STRIPE_SIZE
    } else {
        req.stripe_size
    };

    // Allocate inode from local range (zero FDB contention)
    let ino = state.ino_alloc.alloc().await?;

    // Stripe offset: spread files across available OSTs for even distribution
    // This is an index into the available_osts vector, not the absolute OST index
    let stripe_offset_in_list = (ino as u32) % ost_count;

    // Select specific OST indices from available OSTs
    // ost_indices contains actual OST index values (e.g., [0, 2, 3]), not list positions
    let mut ost_indices = Vec::new();
    for i in 0..stripe_count {
        let list_idx = ((stripe_offset_in_list + i) % ost_count) as usize;
        ost_indices.push(available_osts[list_idx]);
    }

    // Create replica map if replica_count > 1
    let mut replica_map = Vec::new();

    if req.replica_count > 1 && ost_count > 1 {
        for &primary_ost in &ost_indices {
            let mut replicas = Vec::new();
            let mut rng = thread_rng();

            // Select replica_count-1 random OSTs from available_osts (excluding the primary)
            let mut candidates: Vec<u32> = available_osts
                .iter()
                .filter(|&&idx| idx != primary_ost)
                .copied()
                .collect();

            candidates.shuffle(&mut rng);

            // Take replica_count-1 replicas (or all available if fewer)
            let num_replicas = std::cmp::min(req.replica_count as usize - 1, candidates.len());
            replicas.extend(candidates.iter().take(num_replicas).copied());

            replica_map.push(replicas);
        }
    }

    let layout = StripeLayout {
        stripe_size,
        ost_indices,
        replica_count: req.replica_count,
        replica_map,
    };

    let now = FileMeta::now_secs();
    let meta = FileMeta {
        ino,
        name: name.clone(),
        path: path.clone(),
        is_dir: false,
        size: 0,
        ctime: now,
        mtime: now,
        mode: req.mode,
        uid: req.uid,
        gid: req.gid,
        layout: Some(layout.clone()),
        parent_ino,
        // Mark as pending — invisible to lookups until CommitCreate.
        // This prevents readers from seeing a file whose data is still being written.
        pending: true,
        nlink: 1,
        symlink_target: None,
    };

    // Persist atomically: inode + path + child entry + next_ino in one FDB transaction
    state.store.txn_create(&meta, &path, parent_ino).await?;

    debug!(
        "MDS: created file {path} ino={ino} stripes={} stripe_size={} (pending)",
        layout.ost_indices.len(),
        layout.stripe_size,
    );
    Ok(make_reply(req_id, RpcKind::MetaReply(meta)))
}

/// Handle mkdir operation
pub async fn handle_mkdir(req_id: u64, path: &str, state: &MdsState) -> Result<RpcMessage> {
    let path = path_utils::normalize_path(path);
    let parent = path_utils::parent_path(&path);
    let name = path_utils::basename(&path);

    // Check parent exists
    let parent_ino = state
        .store
        .resolve_path(&parent)
        .await?
        .ok_or_else(|| RustreError::NotFound(format!("parent directory: {parent}")))?;

    // Check not exists
    if (state.store.resolve_path(&path).await?).is_some() {
        return Err(RustreError::AlreadyExists(path));
    }

    // Allocate inode from local range (zero FDB contention)
    let ino = state.ino_alloc.alloc().await?;

    let now = FileMeta::now_secs();
    let (uid, gid) = crate::utils::fid::get_ugid();
    let mode = 0o755; // Default directory mode
    let meta = FileMeta {
        ino,
        name: name.clone(),
        path: path.clone(),
        is_dir: true,
        size: 0,
        ctime: now,
        mtime: now,
        mode,
        uid,
        gid,
        layout: None,
        parent_ino,
        pending: false,
        nlink: 2, // directory: . and parent
        symlink_target: None,
    };

    // Persist atomically: inode + path + child entry + next_ino
    state.store.txn_create(&meta, &path, parent_ino).await?;

    debug!("MDS: created directory {path} ino={ino}");
    Ok(make_reply(req_id, RpcKind::Ok))
}

/// Handle mkdir with specific permissions
pub async fn handle_mkdir_with_perms(
    req_id: u64,
    path: &str,
    mode: u32,
    uid: u32,
    gid: u32,
    state: &MdsState,
) -> Result<RpcMessage> {
    let path = path_utils::normalize_path(path);
    let parent = path_utils::parent_path(&path);
    let name = path_utils::basename(&path);

    // Check parent exists
    let parent_ino = state
        .store
        .resolve_path(&parent)
        .await?
        .ok_or_else(|| RustreError::NotFound(format!("parent directory: {parent}")))?;

    // Check not exists
    if (state.store.resolve_path(&path).await?).is_some() {
        return Err(RustreError::AlreadyExists(path));
    }

    // Allocate inode from local range (zero FDB contention)
    let ino = state.ino_alloc.alloc().await?;

    let now = FileMeta::now_secs();
    let meta = FileMeta {
        ino,
        name: name.clone(),
        path: path.clone(),
        is_dir: true,
        size: 0,
        ctime: now,
        mtime: now,
        mode,
        uid,
        gid,
        layout: None,
        parent_ino,
        pending: false,
        nlink: 2, // directory: . and parent
        symlink_target: None,
    };

    // Persist atomically: inode + path + child entry + next_ino
    state.store.txn_create(&meta, &path, parent_ino).await?;

    debug!("MDS: created directory {path} ino={ino} mode={mode:o} uid={uid} gid={gid}");
    Ok(make_reply(req_id, RpcKind::Ok))
}

/// Handle set permissions operation
pub async fn handle_set_perms(
    req_id: u64,
    path: &str,
    mode: Option<u32>,
    uid: Option<u32>,
    gid: Option<u32>,
    state: &MdsState,
) -> Result<RpcMessage> {
    let path = path_utils::normalize_path(path);

    let ino = state
        .store
        .resolve_path(&path)
        .await?
        .ok_or_else(|| RustreError::NotFound(path.clone()))?;

    let mut meta = state
        .store
        .get_inode(ino)
        .await?
        .ok_or_else(|| RustreError::NotFound(path.clone()))?;

    // Update fields if provided
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
    state.store.set_inode(ino, &meta).await?;

    debug!(
        "MDS: set perms for {path} ino={ino} mode={:?} uid={:?} gid={:?}",
        mode, uid, gid
    );
    Ok(make_reply(req_id, RpcKind::Ok))
}

/// Handle readdir operation
pub async fn handle_readdir(req_id: u64, path: &str, state: &MdsState) -> Result<RpcMessage> {
    let path = path_utils::normalize_path(path);

    let ino = state
        .store
        .resolve_path(&path)
        .await?
        .ok_or_else(|| RustreError::NotFound(path.clone()))?;

    let meta = state
        .store
        .get_inode(ino)
        .await?
        .ok_or_else(|| RustreError::NotFound(path.clone()))?;

    if !meta.is_dir {
        return Err(RustreError::NotADirectory(path));
    }

    // List children via FDB range scan — returns (name, child_ino) pairs
    let child_entries = state.store.list_children(ino).await?;

    let mut entries = Vec::new();
    for (child_name, child_ino) in child_entries {
        if let Some(mut child_meta) = state.store.get_inode(child_ino).await? {
            // Skip pending files — they're not visible until committed
            if !child_meta.pending {
                // Use the name from the child entry (directory entry name),
                // not the inode's canonical name, because hard-linked files
                // can appear under different names in the same directory.
                child_meta.name = child_name;
                entries.push(child_meta);
            }
        }
    }

    Ok(make_reply(req_id, RpcKind::MetaListReply(entries)))
}

/// Handle unlink operation
pub async fn handle_unlink(req_id: u64, path: &str, state: &MdsState) -> Result<RpcMessage> {
    let path = path_utils::normalize_path(path);
    if path == "/" {
        return Err(RustreError::InvalidArgument(
            "cannot remove root directory".into(),
        ));
    }

    // Resolve the parent of the path being unlinked.  For hard links the
    // unlinked path may live in a different directory than the inode's
    // original parent_ino, so we must use the path's actual parent.
    let parent_path = path_utils::parent_path(&path);
    let name = path_utils::basename(&path);
    let parent_ino = state
        .store
        .resolve_path(&parent_path)
        .await?
        .ok_or_else(|| RustreError::NotFound(parent_path.clone()))?;

    let ino = state
        .store
        .resolve_path(&path)
        .await?
        .ok_or_else(|| RustreError::NotFound(path.clone()))?;

    let meta = state
        .store
        .get_inode(ino)
        .await?
        .ok_or_else(|| RustreError::NotFound(path.clone()))?;

    if meta.is_dir {
        // Check directory is empty
        if state.store.has_children(ino).await? {
            return Err(RustreError::DirNotEmpty(path));
        }
    }

    if !meta.is_dir && meta.nlink > 1 {
        // Hard-linked file: decrement nlink, remove this path + child entry,
        // but keep the inode alive for the remaining links.
        let mut updated = meta;
        updated.nlink -= 1;
        updated.mtime = FileMeta::now_secs();
        state
            .store
            .txn_dec_nlink(ino, &updated, &path, &name, parent_ino)
            .await?;
        info!(
            "MDS: unlinked {path} ino={ino} (nlink now {})",
            updated.nlink
        );
    } else {
        // Last link (or directory): fully remove inode + path + child entry
        state
            .store
            .txn_unlink(ino, &path, &name, parent_ino, meta.is_dir)
            .await?;
        info!("MDS: unlinked {path} ino={ino}");
    }
    Ok(make_reply(req_id, RpcKind::Ok))
}

/// Handle set size operation
pub async fn handle_set_size(
    req_id: u64,
    path: &str,
    size: u64,
    state: &MdsState,
) -> Result<RpcMessage> {
    let path = path_utils::normalize_path(path);

    let ino = state
        .store
        .resolve_path(&path)
        .await?
        .ok_or_else(|| RustreError::NotFound(path.clone()))?;

    let mut meta = state
        .store
        .get_inode(ino)
        .await?
        .ok_or_else(|| RustreError::NotFound(path.clone()))?;

    meta.size = size;
    meta.mtime = FileMeta::now_secs();
    state.store.set_inode(ino, &meta).await?;

    Ok(make_reply(req_id, RpcKind::Ok))
}

/// Commit a pending file: set final size, clear pending flag, make it visible.
///
/// This is the second phase of the two-phase create protocol.
/// Uses ino directly — no redundant path resolution.
/// Idempotent: if already committed, returns Ok silently.
pub async fn handle_commit_create(
    req_id: u64,
    ino: u64,
    size: u64,
    state: &MdsState,
) -> Result<RpcMessage> {
    let mut meta = state
        .store
        .get_inode(ino)
        .await?
        .ok_or_else(|| RustreError::NotFound(format!("ino {ino}")))?;

    if !meta.pending {
        // Already committed (idempotent — a retry after network timeout is fine)
        return Ok(make_reply(req_id, RpcKind::Ok));
    }

    meta.size = size;
    meta.mtime = FileMeta::now_secs();
    meta.pending = false;
    state.store.set_inode(ino, &meta).await?;

    debug!("MDS: committed file {} ino={ino} size={size}", meta.path);
    Ok(make_reply(req_id, RpcKind::Ok))
}

/// Abort a pending file: atomically remove the MDS record.
///
/// Called by the client when OSS writes fail, to clean up the pending metadata.
/// Uses ino directly — no redundant path resolution.
/// Only removes if the file is still pending (prevents aborting a committed file).
/// Idempotent: if already gone, returns Ok silently.
pub async fn handle_abort_create(req_id: u64, ino: u64, state: &MdsState) -> Result<RpcMessage> {
    let meta = match state.store.get_inode(ino).await? {
        Some(m) => m,
        None => {
            // Already gone — idempotent
            return Ok(make_reply(req_id, RpcKind::Ok));
        }
    };

    // Only remove if still pending — a committed file must never be aborted
    if !meta.pending {
        return Ok(make_reply(req_id, RpcKind::Ok));
    }

    state
        .store
        .txn_unlink(ino, &meta.path, &meta.name, meta.parent_ino, false)
        .await?;

    debug!("MDS: aborted pending file {} ino={ino}", meta.path);
    Ok(make_reply(req_id, RpcKind::Ok))
}

/// Handle rename operation
pub async fn handle_rename(
    req_id: u64,
    old_path: &str,
    new_path: &str,
    state: &MdsState,
) -> Result<RpcMessage> {
    let old_path = path_utils::normalize_path(old_path);
    let new_path = path_utils::normalize_path(new_path);

    if old_path == "/" {
        return Err(RustreError::InvalidArgument(
            "cannot rename root directory".into(),
        ));
    }
    if new_path == "/" {
        return Err(RustreError::InvalidArgument(
            "cannot rename to root directory".into(),
        ));
    }
    if old_path == new_path {
        // Nothing to do
        return Ok(make_reply(req_id, RpcKind::Ok));
    }

    // Get the inode being renamed
    let ino = state
        .store
        .resolve_path(&old_path)
        .await?
        .ok_or_else(|| RustreError::NotFound(old_path.clone()))?;

    let mut meta = state
        .store
        .get_inode(ino)
        .await?
        .ok_or_else(|| RustreError::NotFound(old_path.clone()))?;

    // Check if new parent directory exists
    let new_parent = path_utils::parent_path(&new_path);
    let new_parent_ino = state
        .store
        .resolve_path(&new_parent)
        .await?
        .ok_or_else(|| RustreError::NotFound(format!("parent directory: {new_parent}")))?;

    let new_parent_meta = state
        .store
        .get_inode(new_parent_ino)
        .await?
        .ok_or_else(|| RustreError::NotFound(new_parent.clone()))?;

    if !new_parent_meta.is_dir {
        return Err(RustreError::NotADirectory(new_parent));
    }

    // Check if new path already exists
    if let Some(existing_ino) = state.store.resolve_path(&new_path).await? {
        // Target exists - need to unlink it first (POSIX rename replaces)
        let existing_meta = state
            .store
            .get_inode(existing_ino)
            .await?
            .ok_or_else(|| RustreError::NotFound(new_path.clone()))?;

        if existing_meta.is_dir {
            // Can't replace directory with non-directory or vice versa
            if !meta.is_dir {
                return Err(RustreError::InvalidArgument(
                    "cannot replace directory with non-directory".into(),
                ));
            }
            // Check if target directory is empty
            if state.store.has_children(existing_ino).await? {
                return Err(RustreError::DirNotEmpty(new_path));
            }
        } else if meta.is_dir {
            return Err(RustreError::InvalidArgument(
                "cannot replace non-directory with directory".into(),
            ));
        }

        // Unlink the existing target
        let existing_name = path_utils::basename(&new_path);
        state
            .store
            .txn_unlink(
                existing_ino,
                &new_path,
                &existing_name,
                existing_meta.parent_ino,
                existing_meta.is_dir,
            )
            .await?;
    }

    // Update metadata
    let old_parent_ino = meta.parent_ino;
    let old_name = path_utils::basename(&old_path);
    let new_name = path_utils::basename(&new_path);
    meta.path = new_path.clone();
    meta.name = new_name.clone();
    meta.parent_ino = new_parent_ino;
    meta.mtime = FileMeta::now_secs();

    // Perform the rename transaction atomically
    state
        .store
        .txn_rename(
            ino,
            &old_path,
            &new_path,
            &old_name,
            &new_name,
            old_parent_ino,
            new_parent_ino,
            &meta,
        )
        .await?;

    debug!("MDS: renamed {old_path} -> {new_path} ino={ino}");
    Ok(make_reply(req_id, RpcKind::Ok))
}

/// Handle hard link operation
pub async fn handle_link(
    req_id: u64,
    ino: u64,
    new_path: &str,
    state: &MdsState,
) -> Result<RpcMessage> {
    let new_path = path_utils::normalize_path(new_path);
    let new_parent = path_utils::parent_path(&new_path);
    let new_name = path_utils::basename(&new_path);

    // Get existing inode
    let mut meta = state
        .store
        .get_inode(ino)
        .await?
        .ok_or_else(|| RustreError::NotFound(format!("ino {ino}")))?;

    // Cannot hard-link directories (POSIX rule)
    if meta.is_dir {
        return Err(RustreError::InvalidArgument(
            "cannot hard-link a directory".into(),
        ));
    }

    // Cannot link pending files
    if meta.pending {
        return Err(RustreError::NotFound(format!("ino {ino} (pending)")));
    }

    // Check new parent exists
    let new_parent_ino = state
        .store
        .resolve_path(&new_parent)
        .await?
        .ok_or_else(|| RustreError::NotFound(format!("parent directory: {new_parent}")))?;

    // Check target doesn't already exist
    if (state.store.resolve_path(&new_path).await?).is_some() {
        return Err(RustreError::AlreadyExists(new_path));
    }

    // Increment nlink
    meta.nlink += 1;
    meta.mtime = FileMeta::now_secs();

    // Atomically: update inode nlink + add new path mapping + add parent-child entry
    state
        .store
        .txn_link(ino, &meta, &new_path, &new_name, new_parent_ino)
        .await?;

    debug!("MDS: linked ino={ino} -> {new_path} (nlink={})", meta.nlink);
    Ok(make_reply(req_id, RpcKind::MetaReply(meta)))
}

/// Handle set-size by inode number (preferred by FUSE client).
///
/// Avoids redundant path resolution when the caller already knows the ino.
pub async fn handle_set_size_by_ino(
    req_id: u64,
    ino: u64,
    size: u64,
    state: &MdsState,
) -> Result<RpcMessage> {
    let mut meta = state
        .store
        .get_inode(ino)
        .await?
        .ok_or_else(|| RustreError::NotFound(format!("ino {ino}")))?;

    meta.size = size;
    meta.mtime = FileMeta::now_secs();
    state.store.set_inode(ino, &meta).await?;

    Ok(make_reply(req_id, RpcKind::Ok))
}

/// Handle mknod — create a regular file (non-pending, immediately visible).
///
/// Used as a fallback for platforms where O_CREAT triggers mknod instead of
/// the FUSE create callback. The file is created with size 0 and no stripe
/// layout; a layout is assigned lazily on the first write via the FUSE client.
pub async fn handle_mknod(
    req_id: u64,
    path: &str,
    mode: u32,
    uid: u32,
    gid: u32,
    state: &MdsState,
) -> Result<RpcMessage> {
    let path = path_utils::normalize_path(path);
    let parent = path_utils::parent_path(&path);
    let name = path_utils::basename(&path);

    // Check parent exists and is a directory
    let parent_ino = state
        .store
        .resolve_path(&parent)
        .await?
        .ok_or_else(|| RustreError::NotFound(format!("parent directory: {parent}")))?;

    let parent_meta = state
        .store
        .get_inode(parent_ino)
        .await?
        .ok_or_else(|| RustreError::NotFound(parent.clone()))?;

    if !parent_meta.is_dir {
        return Err(RustreError::NotADirectory(parent));
    }

    // Check if already exists
    if (state.store.resolve_path(&path).await?).is_some() {
        return Err(RustreError::AlreadyExists(path));
    }

    // Fetch latest OST info to compute stripe layout
    let latest_config = fetch_config(&state.mgs_addr).await?;
    {
        let mut config_write = state.cluster_config.write().await;
        *config_write = latest_config.clone();
    }

    let config_read = state.cluster_config.read().await;
    let available_osts: Vec<u32> = config_read
        .ost_list
        .iter()
        .map(|ost| ost.ost_index)
        .collect();
    let ost_count = available_osts.len() as u32;
    if ost_count == 0 {
        return Err(RustreError::NoOstAvailable);
    }

    let ino = state.ino_alloc.alloc().await?;

    // Assign a layout immediately (same logic as handle_create)
    let stripe_count = ost_count;
    let stripe_size = DEFAULT_STRIPE_SIZE;
    let stripe_offset_in_list = (ino as u32) % ost_count;
    let mut ost_indices = Vec::new();
    for i in 0..stripe_count {
        let list_idx = ((stripe_offset_in_list + i) % ost_count) as usize;
        ost_indices.push(available_osts[list_idx]);
    }

    let layout = StripeLayout {
        stripe_size,
        ost_indices,
        replica_count: 1,
        replica_map: Vec::new(),
    };

    let now = FileMeta::now_secs();
    let meta = FileMeta {
        ino,
        name: name.clone(),
        path: path.clone(),
        is_dir: false,
        size: 0,
        ctime: now,
        mtime: now,
        mode,
        uid,
        gid,
        layout: Some(layout),
        parent_ino,
        pending: false, // Immediately visible (unlike Create which starts pending)
        nlink: 1,
        symlink_target: None,
    };

    state.store.txn_create(&meta, &path, parent_ino).await?;

    debug!(
        "MDS: mknod file {path} ino={ino} mode={mode:o} uid={uid} gid={gid}"
    );
    Ok(make_reply(req_id, RpcKind::MetaReply(meta)))
}

/// Handle symbolic link operation
pub async fn handle_symlink(
    req_id: u64,
    path: &str,
    target: &str,
    state: &MdsState,
) -> Result<RpcMessage> {
    let path = path_utils::normalize_path(path);
    let parent = path_utils::parent_path(&path);
    let name = path_utils::basename(&path);

    // Check parent exists and is a directory
    let parent_ino = state
        .store
        .resolve_path(&parent)
        .await?
        .ok_or_else(|| RustreError::NotFound(format!("parent directory: {parent}")))?;

    let parent_meta = state
        .store
        .get_inode(parent_ino)
        .await?
        .ok_or_else(|| RustreError::NotFound(parent.clone()))?;

    if !parent_meta.is_dir {
        return Err(RustreError::NotADirectory(parent));
    }

    // Check if already exists
    if (state.store.resolve_path(&path).await?).is_some() {
        return Err(RustreError::AlreadyExists(path));
    }

    // Allocate inode from local range
    let ino = state.ino_alloc.alloc().await?;

    let now = FileMeta::now_secs();
    let (uid, gid) = crate::utils::fid::get_ugid();
    let mode = 0o777; // Symlinks have all permissions (0777)
    let meta = FileMeta {
        ino,
        name: name.clone(),
        path: path.clone(),
        is_dir: false,
        size: 0,
        ctime: now,
        mtime: now,
        mode,
        uid,
        gid,
        layout: None, // Symlinks have no data on OSS
        parent_ino,
        pending: false, // Symlinks are immediately visible
        nlink: 1,
        symlink_target: Some(target.to_string()),
    };

    // Persist atomically: inode + path + child entry
    state.store.txn_create(&meta, &path, parent_ino).await?;

    debug!("MDS: created symlink {path} -> {target} ino={ino}");
    Ok(make_reply(req_id, RpcKind::MetaReply(meta)))
}
