//! MGS server main logic

use crate::error::Result;
use crate::mgs::{config, heartbeat};
use crate::rpc::{make_reply, recv_msg, send_msg, RpcKind};
use crate::storage::FdbMetaStore;
use crate::types::{OstInfo, StatusInfo};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tracing::{error, info, warn};

/// MGS runtime state — handle to FDB store plus heartbeat tracking.
struct MgsState {
    store: Arc<FdbMetaStore>,
    heartbeat_tracker: Arc<RwLock<heartbeat::HeartbeatTracker>>,
}

/// Main entry point for MGS server
pub async fn run(listen: &str, cluster_name: &str) -> Result<()> {
    let store = Arc::new(FdbMetaStore::new(cluster_name)?);
    let state = Arc::new(MgsState {
        store,
        heartbeat_tracker: Arc::new(RwLock::new(heartbeat::HeartbeatTracker::default())),
    });

    // Spawn heartbeat scanning task
    {
        let store = Arc::clone(&state.store);
        let heartbeat_tracker = Arc::clone(&state.heartbeat_tracker);
        tokio::spawn(async move {
            heartbeat::heartbeat_scan_loop(store, heartbeat_tracker).await;
        });
    }

    let listener = TcpListener::bind(listen).await?;
    info!("MGS listening on {listen} (cluster: {cluster_name}, FoundationDB-backed, stateless HA, heartbeat enabled)");

    loop {
        let (stream, addr) = listener.accept().await?;
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, state).await {
                warn!("MGS: error handling connection from {addr}: {e}");
            }
        });
    }
}

async fn handle_connection(mut stream: TcpStream, state: Arc<MgsState>) -> Result<()> {
    let msg = recv_msg(&mut stream).await?;
    let reply = match msg.kind {
        RpcKind::RegisterMds(mds_info) => {
            // Store MDS keyed by address for dedup
            let key = format!("mds/{}", mds_info.address);
            if let Err(e) = state.store.set(&key, &mds_info).await {
                error!("MGS: failed to save MDS registration: {e}");
            } else {
                info!("MGS: registered MDS at {}", mds_info.address);
            }
            make_reply(msg.id, RpcKind::Ok)
        }

        RpcKind::RegisterOst(ost_info) => {
            // Store OST keyed by zero-padded index for ordered iteration
            let key = format!("ost/{:08x}", ost_info.ost_index);
            if let Err(e) = state.store.set(&key, &ost_info).await {
                error!("MGS: failed to save OST registration: {e}");
            } else {
                info!(
                    "MGS: registered OST {} at {}",
                    ost_info.ost_index, ost_info.address
                );
            }
            make_reply(msg.id, RpcKind::Ok)
        }

        RpcKind::UpdateOstUsage {
            ost_index,
            used_bytes,
        } => {
            // Update usage in a separate key to avoid conflicts with registration
            let key = format!("ost_usage/{:08x}", ost_index);
            let _ = state.store.set(&key, &used_bytes).await;

            // Also update the main OST entry
            let ost_key = format!("ost/{:08x}", ost_index);
            if let Ok(Some(mut ost_info)) = state.store.get::<OstInfo>(&ost_key).await {
                ost_info.used_bytes = used_bytes;
                let _ = state.store.set(&ost_key, &ost_info).await;
            }

            make_reply(msg.id, RpcKind::Ok)
        }

        RpcKind::GetConfig => match config::build_cluster_config(&state.store).await {
            Ok(config) => make_reply(msg.id, RpcKind::ConfigReply(config)),
            Err(e) => make_reply(msg.id, RpcKind::Error(format!("config read failed: {e}"))),
        },

        RpcKind::GetStatus => match config::build_cluster_config(&state.store).await {
            Ok(config) => {
                let status = StatusInfo {
                    mds_count: config.mds_list.len(),
                    ost_count: config.ost_list.len(),
                    osts: config.ost_list,
                    mds_list: config.mds_list,
                };
                make_reply(msg.id, RpcKind::StatusReply(status))
            }
            Err(e) => make_reply(msg.id, RpcKind::Error(format!("status read failed: {e}"))),
        },

        RpcKind::AllocInodeRange { count } => match state.store.alloc_inode_range(count).await {
            Ok((start, end)) => {
                info!("MGS: allocated inode range [{start}, {end}) ({count} inodes)");
                make_reply(msg.id, RpcKind::InodeRangeReply { start, end })
            }
            Err(e) => {
                error!("MGS: failed to allocate inode range: {e}");
                make_reply(msg.id, RpcKind::Error(format!("alloc inode range: {e}")))
            }
        },

        // ReturnInodeRange is deprecated — inode space is effectively infinite,
        // no reclaim needed. Accept silently for wire compatibility.
        RpcKind::ReturnInodeRange { start, end } => {
            info!("MGS: ignoring returned inode range [{start}, {end}) (deprecated, no reclaim)");
            make_reply(msg.id, RpcKind::Ok)
        }

        other => {
            warn!("MGS: unexpected RPC: {other:?}");
            make_reply(
                msg.id,
                RpcKind::Error("unsupported operation for MGS".into()),
            )
        }
    };
    send_msg(&mut stream, &reply).await
}
