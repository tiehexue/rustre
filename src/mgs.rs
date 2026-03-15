//! Management Server (MGS) — now backed by FoundationDB for stateless HA.
//!
//! The MGS is the cluster's configuration authority. It:
//! - Accepts registrations from MDS and OSS nodes
//! - Maintains the authoritative cluster configuration in FoundationDB
//! - Serves configuration to clients so they know where MDS/OSS live
//!
//! Because all state lives in FDB, multiple MGS instances can run
//! simultaneously for high availability — they are pure stateless proxies.

use crate::common::*;
use crate::net::{make_reply, recv_msg, send_msg};
use crate::storage::FdbMetaStore;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, warn};

/// MGS runtime state — just a handle to the shared FDB store.
struct MgsState {
    store: FdbMetaStore,
}

pub async fn run(listen: &str, cluster_name: &str) -> Result<()> {
    let store = FdbMetaStore::new(cluster_name)?;
    let state = Arc::new(MgsState { store });

    let listener = TcpListener::bind(listen).await?;
    info!("MGS listening on {listen} (cluster: {cluster_name}, FoundationDB-backed, stateless HA)");

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

        RpcKind::GetConfig => match build_cluster_config(&state.store).await {
            Ok(config) => make_reply(msg.id, RpcKind::ConfigReply(config)),
            Err(e) => make_reply(msg.id, RpcKind::Error(format!("config read failed: {e}"))),
        },

        RpcKind::GetStatus => match build_cluster_config(&state.store).await {
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

/// Build a ClusterConfig by reading all MDS and OST entries from FDB.
async fn build_cluster_config(store: &FdbMetaStore) -> Result<ClusterConfig> {
    let mds_list: Vec<MdsInfo> = store.list_prefix("mds/").await?;
    let mut ost_list: Vec<OstInfo> = store.list_prefix("ost/").await?;
    // Ensure sorted by ost_index for deterministic striping
    ost_list.sort_by_key(|o| o.ost_index);

    Ok(ClusterConfig { mds_list, ost_list })
}
