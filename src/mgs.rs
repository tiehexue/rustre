//! Management Server (MGS)
//!
//! The MGS is the cluster's configuration authority. It:
//! - Accepts registrations from MDS and OSS nodes
//! - Maintains the authoritative cluster configuration
//! - Serves configuration to clients so they know where MDS/OSS live
//! - Persists config to disk for crash recovery

use crate::common::*;
use crate::net::{make_reply, recv_msg, send_msg};
use crate::storage::MetaStore;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tracing::{error, info, warn};

struct MgsState {
    config: ClusterConfig,
    store: MetaStore,
}

impl MgsState {
    async fn save_config(&self) -> Result<()> {
        self.store.save("cluster_config", &self.config).await
    }
}

pub async fn run(listen: &str, data_dir: &str) -> Result<()> {
    let store = MetaStore::new(data_dir).await?;

    // Try to load existing config
    let config = store
        .load::<ClusterConfig>("cluster_config")
        .await
        .unwrap_or_default();

    let state = Arc::new(RwLock::new(MgsState { config, store }));

    let listener = TcpListener::bind(listen).await?;
    info!("MGS listening on {listen}");

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

async fn handle_connection(mut stream: TcpStream, state: Arc<RwLock<MgsState>>) -> Result<()> {
    let msg = recv_msg(&mut stream).await?;
    let reply = match msg.kind {
        RpcKind::RegisterMds(mds_info) => {
            let mut st = state.write().await;
            // Avoid duplicates
            if !st
                .config
                .mds_list
                .iter()
                .any(|m| m.address == mds_info.address)
            {
                info!("MGS: registered MDS at {}", mds_info.address);
                st.config.mds_list.push(mds_info);
                if let Err(e) = st.save_config().await {
                    error!("MGS: failed to save config: {e}");
                }
            } else {
                info!("MGS: MDS at {} already registered", mds_info.address);
            }
            make_reply(msg.id, RpcKind::Ok)
        }

        RpcKind::RegisterOst(ost_info) => {
            let mut st = state.write().await;
            // Update or insert
            if let Some(existing) = st
                .config
                .ost_list
                .iter_mut()
                .find(|o| o.ost_index == ost_info.ost_index)
            {
                existing.address = ost_info.address.clone();
                existing.total_bytes = ost_info.total_bytes;
                existing.used_bytes = ost_info.used_bytes;
                info!(
                    "MGS: updated OST {} at {}",
                    ost_info.ost_index, ost_info.address
                );
            } else {
                info!(
                    "MGS: registered OST {} at {}",
                    ost_info.ost_index, ost_info.address
                );
                st.config.ost_list.push(ost_info);
            }
            // Sort by index for deterministic striping
            st.config.ost_list.sort_by_key(|o| o.ost_index);
            if let Err(e) = st.save_config().await {
                error!("MGS: failed to save config: {e}");
            }
            make_reply(msg.id, RpcKind::Ok)
        }

        RpcKind::UpdateOstUsage {
            ost_index,
            used_bytes,
        } => {
            let mut st = state.write().await;
            if let Some(ost) = st
                .config
                .ost_list
                .iter_mut()
                .find(|o| o.ost_index == ost_index)
            {
                ost.used_bytes = used_bytes;
            }
            make_reply(msg.id, RpcKind::Ok)
        }

        RpcKind::GetConfig => {
            let st = state.read().await;
            make_reply(msg.id, RpcKind::ConfigReply(st.config.clone()))
        }

        RpcKind::GetStatus => {
            let st = state.read().await;
            let status = StatusInfo {
                mds_count: st.config.mds_list.len(),
                ost_count: st.config.ost_list.len(),
                osts: st.config.ost_list.clone(),
                mds_list: st.config.mds_list.clone(),
            };
            make_reply(msg.id, RpcKind::StatusReply(status))
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
