//! Metadata Server (MDS)
//!
//! The MDS manages the filesystem namespace:
//! - Inode allocation and management
//! - Directory hierarchy (lookup, create, mkdir, readdir, unlink)
//! - Stripe layout computation for new files (round-robin across OSTs)
//! - Registers itself with the MGS on startup
//!
//! All metadata is persisted in FoundationDB via `FdbMdsStore`.
//! This makes MDS completely stateless — multiple MDS instances can run
//! concurrently for high availability. Any MDS can serve any request since
//! all state lives in FDB with transactional consistency.
//!
//! Stripe layouts are simplified — no per-object list, just parameters.
//! Object IDs are deterministic: derived from (ino, stripe_seq).

use crate::common::*;
use crate::net::{make_reply, recv_msg, rpc_call, send_msg};
use crate::storage::FdbMdsStore;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tracing::{info, warn};

struct MdsState {
    store: FdbMdsStore,
    cluster_config: ClusterConfig,
    #[allow(dead_code)]
    mgs_addr: String,
}

#[allow(dead_code)]
const ROOT_INO: u64 = 1;

pub async fn run(listen: &str, mgs_addr: &str, cluster_name: &str) -> Result<()> {
    let store = FdbMdsStore::new(cluster_name)?;

    // Ensure root directory exists in FDB (idempotent)
    store.ensure_root().await?;

    // Fetch cluster config from MGS
    let config = fetch_config(mgs_addr).await?;

    let state = Arc::new(RwLock::new(MdsState {
        store,
        cluster_config: config,
        mgs_addr: mgs_addr.to_string(),
    }));

    // Register with MGS
    register_with_mgs(mgs_addr, listen).await?;

    // Spawn a background task to refresh cluster config periodically
    {
        let state = Arc::clone(&state);
        let mgs = mgs_addr.to_string();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                if let Ok(cfg) = fetch_config(&mgs).await {
                    let mut st = state.write().await;
                    st.cluster_config = cfg;
                }
            }
        });
    }

    let listener = TcpListener::bind(listen).await?;
    info!("MDS listening on {listen} (stateless, backed by FoundationDB cluster={cluster_name})");

    loop {
        let (stream, addr) = listener.accept().await?;
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, state).await {
                warn!("MDS: error handling connection from {addr}: {e}");
            }
        });
    }
}

async fn fetch_config(mgs_addr: &str) -> Result<ClusterConfig> {
    let reply = rpc_call(mgs_addr, RpcKind::GetConfig).await?;
    match reply.kind {
        RpcKind::ConfigReply(cfg) => Ok(cfg),
        RpcKind::Error(e) => Err(RustreError::Net(e)),
        _ => Err(RustreError::Net("unexpected reply from MGS".into())),
    }
}

async fn register_with_mgs(mgs_addr: &str, listen: &str) -> Result<()> {
    // Convert "0.0.0.0:port" to "127.0.0.1:port" for local registration
    let addr = if listen.starts_with("0.0.0.0") {
        listen.replace("0.0.0.0", "127.0.0.1")
    } else {
        listen.to_string()
    };
    let reply = rpc_call(
        mgs_addr,
        RpcKind::RegisterMds(MdsInfo {
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

async fn handle_connection(mut stream: TcpStream, state: Arc<RwLock<MdsState>>) -> Result<()> {
    let msg = recv_msg(&mut stream).await?;
    let reply = match msg.kind {
        RpcKind::Lookup(path) => handle_lookup(msg.id, &path, &state).await,
        RpcKind::Create(req) => handle_create(msg.id, req, &state).await,
        RpcKind::Mkdir(path) => handle_mkdir(msg.id, &path, &state).await,
        RpcKind::Readdir(path) => handle_readdir(msg.id, &path, &state).await,
        RpcKind::Unlink(path) => handle_unlink(msg.id, &path, &state).await,
        RpcKind::Stat(path) => handle_lookup(msg.id, &path, &state).await,
        RpcKind::SetSize { path, size } => handle_set_size(msg.id, &path, size, &state).await,
        other => {
            warn!("MDS: unexpected RPC: {other:?}");
            Ok(make_reply(
                msg.id,
                RpcKind::Error("unsupported operation for MDS".into()),
            ))
        }
    };
    match reply {
        Ok(r) => send_msg(&mut stream, &r).await,
        Err(e) => {
            let err_str = e.to_string();
            let r = make_reply(msg.id, RpcKind::Error(err_str));
            send_msg(&mut stream, &r).await
        }
    }
}

// Normalize path: ensure leading /, remove trailing / (except root)
fn normalize_path(p: &str) -> String {
    let p = if p.starts_with('/') {
        p.to_string()
    } else {
        format!("/{p}")
    };
    if p.len() > 1 && p.ends_with('/') {
        p.trim_end_matches('/').to_string()
    } else {
        p
    }
}

fn parent_path(p: &str) -> String {
    if p == "/" {
        return "/".to_string();
    }
    match p.rfind('/') {
        Some(0) => "/".to_string(),
        Some(idx) => p[..idx].to_string(),
        None => "/".to_string(),
    }
}

fn basename(p: &str) -> String {
    if p == "/" {
        return "/".to_string();
    }
    p.rsplit('/').next().unwrap_or("").to_string()
}

// ---------------------------------------------------------------------------
// Handlers — all metadata ops go through FdbMdsStore
// ---------------------------------------------------------------------------

async fn handle_lookup(req_id: u64, path: &str, state: &RwLock<MdsState>) -> Result<RpcMessage> {
    let path = normalize_path(path);
    let st = state.read().await;

    let ino = st
        .store
        .resolve_path(&path)
        .await?
        .ok_or_else(|| RustreError::NotFound(path.clone()))?;

    let meta = st
        .store
        .get_inode(ino)
        .await?
        .ok_or_else(|| RustreError::NotFound(path.clone()))?;

    Ok(make_reply(req_id, RpcKind::MetaReply(meta)))
}

async fn handle_create(
    req_id: u64,
    req: CreateReq,
    state: &RwLock<MdsState>,
) -> Result<RpcMessage> {
    let path = normalize_path(&req.path);
    let parent = parent_path(&path);
    let name = basename(&path);

    let st = state.read().await;

    // Check parent exists and is a directory
    let parent_ino = st
        .store
        .resolve_path(&parent)
        .await?
        .ok_or_else(|| RustreError::NotFound(format!("parent directory: {parent}")))?;

    let parent_meta = st
        .store
        .get_inode(parent_ino)
        .await?
        .ok_or_else(|| RustreError::NotFound(parent.clone()))?;

    if !parent_meta.is_dir {
        return Err(RustreError::NotADirectory(parent));
    }

    // Check if already exists
    if (st.store.resolve_path(&path).await?).is_some() {
        return Err(RustreError::AlreadyExists(path));
    }

    // Compute stripe layout
    let ost_count = st.cluster_config.ost_list.len() as u32;
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

    // Allocate inode atomically from FDB
    let ino = st.store.alloc_ino().await?;

    // Stripe offset: spread files across OSTs for even distribution
    let stripe_offset = (ino as u32) % ost_count;

    // Select specific OST indices when stripe_count < total OST count
    let mut ost_indices = Vec::new();
    if stripe_count < ost_count {
        // Select OST indices starting from stripe_offset, wrapping around
        for i in 0..stripe_count {
            let ost_idx = (stripe_offset + i) % ost_count;
            ost_indices.push(ost_idx);
        }
    }

    let layout = StripeLayout {
        stripe_count,
        stripe_size,
        stripe_offset,
        ost_indices,
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
        layout: Some(layout.clone()),
        parent_ino,
    };

    // Persist atomically: inode + path + child entry + next_ino in one FDB transaction
    st.store.txn_create(&meta, &path, parent_ino).await?;

    info!(
        "MDS: created file {path} ino={ino} stripes={} stripe_size={} offset={}",
        layout.stripe_count, layout.stripe_size, layout.stripe_offset
    );
    Ok(make_reply(req_id, RpcKind::MetaReply(meta)))
}

async fn handle_mkdir(req_id: u64, path: &str, state: &RwLock<MdsState>) -> Result<RpcMessage> {
    let path = normalize_path(path);
    let parent = parent_path(&path);
    let name = basename(&path);

    let st = state.read().await;

    // Check parent exists
    let parent_ino = st
        .store
        .resolve_path(&parent)
        .await?
        .ok_or_else(|| RustreError::NotFound(format!("parent directory: {parent}")))?;

    // Check not exists
    if (st.store.resolve_path(&path).await?).is_some() {
        return Err(RustreError::AlreadyExists(path));
    }

    // Allocate inode atomically from FDB
    let ino = st.store.alloc_ino().await?;

    let now = FileMeta::now_secs();
    let meta = FileMeta {
        ino,
        name: name.clone(),
        path: path.clone(),
        is_dir: true,
        size: 0,
        ctime: now,
        mtime: now,
        layout: None,
        parent_ino,
    };

    // Persist atomically: inode + path + child entry + next_ino
    st.store.txn_create(&meta, &path, parent_ino).await?;

    info!("MDS: created directory {path} ino={ino}");
    Ok(make_reply(req_id, RpcKind::Ok))
}

async fn handle_readdir(req_id: u64, path: &str, state: &RwLock<MdsState>) -> Result<RpcMessage> {
    let path = normalize_path(path);
    let st = state.read().await;

    let ino = st
        .store
        .resolve_path(&path)
        .await?
        .ok_or_else(|| RustreError::NotFound(path.clone()))?;

    let meta = st
        .store
        .get_inode(ino)
        .await?
        .ok_or_else(|| RustreError::NotFound(path.clone()))?;

    if !meta.is_dir {
        return Err(RustreError::NotADirectory(path));
    }

    // List children via FDB range scan
    let child_inos = st.store.list_children(ino).await?;

    let mut entries = Vec::new();
    for child_ino in child_inos {
        if let Some(child_meta) = st.store.get_inode(child_ino).await? {
            entries.push(child_meta);
        }
    }

    Ok(make_reply(req_id, RpcKind::MetaListReply(entries)))
}

async fn handle_unlink(req_id: u64, path: &str, state: &RwLock<MdsState>) -> Result<RpcMessage> {
    let path = normalize_path(path);
    if path == "/" {
        return Err(RustreError::InvalidArgument(
            "cannot remove root directory".into(),
        ));
    }

    let st = state.read().await;

    let ino = st
        .store
        .resolve_path(&path)
        .await?
        .ok_or_else(|| RustreError::NotFound(path.clone()))?;

    let meta = st
        .store
        .get_inode(ino)
        .await?
        .ok_or_else(|| RustreError::NotFound(path.clone()))?;

    if meta.is_dir {
        // Check directory is empty
        if st.store.has_children(ino).await? {
            return Err(RustreError::DirNotEmpty(path));
        }
    }

    // Atomically remove: inode + path + parent's child entry (+ children range for dirs)
    st.store
        .txn_unlink(ino, &path, meta.parent_ino, meta.is_dir)
        .await?;

    info!("MDS: unlinked {path} ino={ino}");
    Ok(make_reply(req_id, RpcKind::Ok))
}

async fn handle_set_size(
    req_id: u64,
    path: &str,
    size: u64,
    state: &RwLock<MdsState>,
) -> Result<RpcMessage> {
    let path = normalize_path(path);
    let st = state.read().await;

    let ino = st
        .store
        .resolve_path(&path)
        .await?
        .ok_or_else(|| RustreError::NotFound(path.clone()))?;

    let mut meta = st
        .store
        .get_inode(ino)
        .await?
        .ok_or_else(|| RustreError::NotFound(path.clone()))?;

    meta.size = size;
    meta.mtime = FileMeta::now_secs();
    st.store.set_inode(ino, &meta).await?;

    Ok(make_reply(req_id, RpcKind::Ok))
}
