//! MDS server main logic

use crate::error::Result;
use crate::mds::operations;
use crate::rpc::{make_reply, recv_msg, send_msg, RpcKind};
use crate::storage::FdbMdsStore;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Main entry point for MDS server
pub async fn run(listen: &str, mgs_addr: &str, cluster_name: &str) -> Result<()> {
    let store = Arc::new(FdbMdsStore::new(cluster_name)?);

    // Ensure root directory exists in FDB (idempotent)
    store.ensure_root().await?;

    // Fetch cluster config from MGS
    let config = operations::fetch_config(mgs_addr).await?;

    // Request initial inode range from MGS
    let (ino_start, ino_end) = operations::alloc_initial_inode_range(mgs_addr).await?;
    let ino_alloc = Arc::new(operations::InodeAllocator::new(
        ino_start,
        ino_end,
        mgs_addr.to_string(),
    ));

    let state = Arc::new(operations::MdsState {
        store,
        cluster_config: RwLock::new(config),
        mgs_addr: mgs_addr.to_string(),
        ino_alloc,
    });

    // Register with MGS
    operations::register_with_mgs(mgs_addr, listen).await?;

    // Spawn a background task to refresh cluster config periodically
    {
        let state = Arc::clone(&state);
        let mgs = mgs_addr.to_string();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                if let Ok(cfg) = operations::fetch_config(&mgs).await {
                    let mut config_write = state.cluster_config.write().await;
                    *config_write = cfg;
                }
            }
        });
    }

    let listener = TcpListener::bind(listen).await?;
    info!("MDS listening on {listen} (stateless, backed by FoundationDB cluster={cluster_name}, inode range [{ino_start}, {ino_end}))");

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

async fn handle_connection(mut stream: TcpStream, state: Arc<operations::MdsState>) -> Result<()> {
    let msg = recv_msg(&mut stream).await?;
    let reply = match msg.kind {
        RpcKind::Lookup(path) => operations::handle_lookup(msg.id, &path, &state).await,
        RpcKind::Create(req) => operations::handle_create(msg.id, req, &state).await,
        RpcKind::Mkdir(path) => operations::handle_mkdir(msg.id, &path, &state).await,
        RpcKind::MkdirWithPerms {
            path,
            mode,
            uid,
            gid,
        } => operations::handle_mkdir_with_perms(msg.id, &path, mode, uid, gid, &state).await,
        RpcKind::SetPerms {
            path,
            mode,
            uid,
            gid,
        } => operations::handle_set_perms(msg.id, &path, mode, uid, gid, &state).await,
        RpcKind::Readdir(path) => operations::handle_readdir(msg.id, &path, &state).await,
        RpcKind::Unlink(path) => operations::handle_unlink(msg.id, &path, &state).await,
        RpcKind::Stat(path) => operations::handle_lookup(msg.id, &path, &state).await,
        RpcKind::SetSize { path, size } => {
            operations::handle_set_size(msg.id, &path, size, &state).await
        }
        RpcKind::CommitCreate { ino, size } => {
            operations::handle_commit_create(msg.id, ino, size, &state).await
        }
        RpcKind::AbortCreate { ino } => operations::handle_abort_create(msg.id, ino, &state).await,
        RpcKind::Rename { old_path, new_path } => {
            operations::handle_rename(msg.id, &old_path, &new_path, &state).await
        }
        RpcKind::Link { ino, new_path } => {
            operations::handle_link(msg.id, ino, &new_path, &state).await
        }
        RpcKind::Symlink { path, target } => {
            operations::handle_symlink(msg.id, &path, &target, &state).await
        }
        RpcKind::Heartbeat => {
            // Respond to heartbeat immediately
            Ok(make_reply(msg.id, RpcKind::HeartbeatReply))
        }
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
