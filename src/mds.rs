//! Metadata Server (MDS)
//!
//! The MDS manages the filesystem namespace:
//! - Inode allocation and management
//! - Directory hierarchy (lookup, create, mkdir, readdir, unlink)
//! - Stripe layout computation for new files (round-robin across OSTs)
//! - Registers itself with the MGS on startup
//!
//! On disk, metadata is persisted via MetaStore (JSON files).

use crate::common::*;
use crate::net::{make_reply, recv_msg, rpc_call, send_msg};
use crate::storage::MetaStore;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tracing::{info, warn};

struct MdsState {
    store: MetaStore,
    next_ino: AtomicU64,
    cluster_config: ClusterConfig,
#[allow(dead_code)]
    mgs_addr: String,
}

const ROOT_INO: u64 = 1;

pub async fn run(listen: &str, mgs_addr: &str, data_dir: &str) -> Result<()> {
    let store = MetaStore::new(data_dir).await?;

    // Ensure root directory exists
    if !store.exists("inode_1").await {
        let root = FileMeta {
            ino: ROOT_INO,
            name: "/".into(),
            path: "/".into(),
            is_dir: true,
            size: 0,
            ctime: FileMeta::now_secs(),
            mtime: FileMeta::now_secs(),
            layout: None,
            parent_ino: 0,
        };
        store.save("inode_1", &root).await?;
        // Root directory children list
        store.save::<Vec<u64>>("children_1", &vec![]).await?;
        store.save("next_ino", &2u64).await?;
        // Path → inode mapping
        store.save("path_/", &ROOT_INO).await?;
    }

    let next_ino_val: u64 = store.load("next_ino").await.unwrap_or(2);

    // Fetch cluster config from MGS
    let config = fetch_config(mgs_addr).await?;

    let state = Arc::new(RwLock::new(MdsState {
        store,
        next_ino: AtomicU64::new(next_ino_val),
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
    info!("MDS listening on {listen}");

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

async fn handle_connection(
    mut stream: TcpStream,
    state: Arc<RwLock<MdsState>>,
) -> Result<()> {
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

async fn handle_lookup(
    req_id: u64,
    path: &str,
    state: &RwLock<MdsState>,
) -> Result<RpcMessage> {
    let path = normalize_path(path);
    let st = state.read().await;
    let ino: u64 = st
        .store
        .load(&format!("path_{path}"))
        .await
        .map_err(|_| RustreError::NotFound(path.clone()))?;
    let meta: FileMeta = st
        .store
        .load(&format!("inode_{ino}"))
        .await
        .map_err(|_| RustreError::NotFound(path.clone()))?;
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

    let st = state.write().await;

    // Check parent exists and is a directory
    let parent_ino: u64 = st
        .store
        .load(&format!("path_{parent}"))
        .await
        .map_err(|_| RustreError::NotFound(format!("parent directory: {parent}")))?;
    let parent_meta: FileMeta = st
        .store
        .load(&format!("inode_{parent_ino}"))
        .await
        .map_err(|_| RustreError::NotFound(parent.clone()))?;
    if !parent_meta.is_dir {
        return Err(RustreError::NotADirectory(parent));
    }

    // Check if already exists
    if st.store.exists(&format!("path_{path}")).await {
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
        1_048_576 // 1 MiB default
    } else {
        req.stripe_size
    };

    // Round-robin OST assignment starting from a pseudo-random offset
    let ino = st.next_ino.fetch_add(1, Ordering::Relaxed);
    let stripe_offset = (ino as u32) % ost_count;

    // Create stripe objects (one per OST in the stripe)
    let mut objects = Vec::with_capacity(stripe_count as usize);
    for i in 0..stripe_count {
        let ost_idx = (stripe_offset + i) % ost_count;
        let object_id = format!("{:016x}_{ost_idx}", ino);
        objects.push(StripeObject {
            object_id,
            ost_index: ost_idx,
            offset: 0, // Will be filled by client during write
            length: 0,
        });
    }

    let layout = StripeLayout {
        stripe_count,
        stripe_size,
        stripe_offset,
        objects,
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
        layout: Some(layout),
        parent_ino,
    };

    // Persist
    st.store.save(&format!("inode_{ino}"), &meta).await?;
    st.store.save(&format!("path_{path}"), &ino).await?;
    st.store
        .save("next_ino", &(ino + 1))
        .await?;

    // Add to parent's children
    let mut children: Vec<u64> = st
        .store
        .load(&format!("children_{parent_ino}"))
        .await
        .unwrap_or_default();
    children.push(ino);
    st.store
        .save(&format!("children_{parent_ino}"), &children)
        .await?;

    info!("MDS: created file {path} ino={ino} stripes={}", meta.layout.as_ref().unwrap().stripe_count);
    Ok(make_reply(req_id, RpcKind::MetaReply(meta)))
}

async fn handle_mkdir(
    req_id: u64,
    path: &str,
    state: &RwLock<MdsState>,
) -> Result<RpcMessage> {
    let path = normalize_path(path);
    let parent = parent_path(&path);
    let name = basename(&path);

    let st = state.write().await;

    // Check parent
    let parent_ino: u64 = st
        .store
        .load(&format!("path_{parent}"))
        .await
        .map_err(|_| RustreError::NotFound(format!("parent directory: {parent}")))?;

    // Check not exists
    if st.store.exists(&format!("path_{path}")).await {
        return Err(RustreError::AlreadyExists(path));
    }

    let ino = st.next_ino.fetch_add(1, Ordering::Relaxed);
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

    st.store.save(&format!("inode_{ino}"), &meta).await?;
    st.store.save(&format!("path_{path}"), &ino).await?;
    st.store
        .save::<Vec<u64>>(&format!("children_{ino}"), &vec![])
        .await?;
    st.store
        .save("next_ino", &(ino + 1))
        .await?;

    // Add to parent's children
    let mut children: Vec<u64> = st
        .store
        .load(&format!("children_{parent_ino}"))
        .await
        .unwrap_or_default();
    children.push(ino);
    st.store
        .save(&format!("children_{parent_ino}"), &children)
        .await?;

    info!("MDS: created directory {path} ino={ino}");
    Ok(make_reply(req_id, RpcKind::Ok))
}

async fn handle_readdir(
    req_id: u64,
    path: &str,
    state: &RwLock<MdsState>,
) -> Result<RpcMessage> {
    let path = normalize_path(path);
    let st = state.read().await;

    let ino: u64 = st
        .store
        .load(&format!("path_{path}"))
        .await
        .map_err(|_| RustreError::NotFound(path.clone()))?;
    let meta: FileMeta = st
        .store
        .load(&format!("inode_{ino}"))
        .await
        .map_err(|_| RustreError::NotFound(path.clone()))?;
    if !meta.is_dir {
        return Err(RustreError::NotADirectory(path));
    }

    let children: Vec<u64> = st
        .store
        .load(&format!("children_{ino}"))
        .await
        .unwrap_or_default();

    let mut entries = Vec::new();
    for child_ino in children {
        if let Ok(child_meta) = st
            .store
            .load::<FileMeta>(&format!("inode_{child_ino}"))
            .await
        {
            entries.push(child_meta);
        }
    }

    Ok(make_reply(req_id, RpcKind::MetaListReply(entries)))
}

async fn handle_unlink(
    req_id: u64,
    path: &str,
    state: &RwLock<MdsState>,
) -> Result<RpcMessage> {
    let path = normalize_path(path);
    if path == "/" {
        return Err(RustreError::InvalidArgument(
            "cannot remove root directory".into(),
        ));
    }

    let st = state.write().await;

    let ino: u64 = st
        .store
        .load(&format!("path_{path}"))
        .await
        .map_err(|_| RustreError::NotFound(path.clone()))?;
    let meta: FileMeta = st
        .store
        .load(&format!("inode_{ino}"))
        .await
        .map_err(|_| RustreError::NotFound(path.clone()))?;

    if meta.is_dir {
        let children: Vec<u64> = st
            .store
            .load(&format!("children_{ino}"))
            .await
            .unwrap_or_default();
        if !children.is_empty() {
            return Err(RustreError::DirNotEmpty(path));
        }
        st.store.delete(&format!("children_{ino}")).await?;
    }

    // Remove from parent's children
    let parent_ino = meta.parent_ino;
    let mut children: Vec<u64> = st
        .store
        .load(&format!("children_{parent_ino}"))
        .await
        .unwrap_or_default();
    children.retain(|&c| c != ino);
    st.store
        .save(&format!("children_{parent_ino}"), &children)
        .await?;

    // Remove inode and path mapping
    st.store.delete(&format!("inode_{ino}")).await?;
    st.store.delete(&format!("path_{path}")).await?;

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
    let st = state.write().await;

    let ino: u64 = st
        .store
        .load(&format!("path_{path}"))
        .await
        .map_err(|_| RustreError::NotFound(path.clone()))?;
    let mut meta: FileMeta = st
        .store
        .load(&format!("inode_{ino}"))
        .await
        .map_err(|_| RustreError::NotFound(path.clone()))?;

    meta.size = size;
    meta.mtime = FileMeta::now_secs();
    st.store.save(&format!("inode_{ino}"), &meta).await?;

    Ok(make_reply(req_id, RpcKind::Ok))
}
