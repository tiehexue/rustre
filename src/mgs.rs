//! Management Server (MGS) — now backed by FoundationDB for stateless HA.
//!
//! The MGS is the cluster's configuration authority. It:
//! - Accepts registrations from MDS and OSS nodes
//! - Maintains the authoritative cluster configuration in FoundationDB
//! - Serves configuration to clients so they know where MDS/OSS live
//! - Sends heartbeats to registered MDS/OSS nodes every second
//! - Removes nodes that fail 2 consecutive heartbeats
//!
//! Because all state lives in FDB, multiple MGS instances can run
//! simultaneously for high availability — they are pure stateless proxies.

use crate::common::*;
use crate::net::{make_reply, recv_msg, send_msg};
use crate::storage::FdbMetaStore;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tokio::time::timeout;
use tracing::{error, info, warn};

/// Heartbeat timeout in seconds
const HEARTBEAT_TIMEOUT_SECS: u64 = 1;
/// Maximum consecutive heartbeat failures before removing a node
const MAX_HEARTBEAT_FAILURES: u32 = 2;

/// Tracks heartbeat failure counts for registered nodes
#[derive(Debug, Default)]
struct HeartbeatTracker {
    /// MDS address -> consecutive failure count
    mds_failures: HashMap<String, u32>,
    /// OST key (ost/{index}) -> (address, consecutive failure count)
    ost_failures: HashMap<String, (String, u32)>,
}

/// MGS runtime state — handle to FDB store plus heartbeat tracking.
struct MgsState {
    store: FdbMetaStore,
    heartbeat_tracker: RwLock<HeartbeatTracker>,
}

pub async fn run(listen: &str, cluster_name: &str) -> Result<()> {
    let store = FdbMetaStore::new(cluster_name)?;
    let state = Arc::new(MgsState {
        store,
        heartbeat_tracker: RwLock::new(HeartbeatTracker::default()),
    });

    // Spawn heartbeat scanning task
    {
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            heartbeat_scan_loop(state).await;
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

/// Background task that sends heartbeats to all registered MDS/OSS every second.
/// If a node fails to respond within 1 second for 2 consecutive attempts, it is removed.
async fn heartbeat_scan_loop(state: Arc<MgsState>) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));

    loop {
        interval.tick().await;

        // Get current list of registered MDS and OST from FDB
        let mds_list: Vec<MdsInfo> = match state.store.list_prefix("mds/").await {
            Ok(list) => list,
            Err(e) => {
                warn!("MGS heartbeat: failed to list MDS: {e}");
                continue;
            }
        };

        let ost_list: Vec<OstInfo> = match state.store.list_prefix("ost/").await {
            Ok(list) => list,
            Err(e) => {
                warn!("MGS heartbeat: failed to list OST: {e}");
                continue;
            }
        };

        // Initialize tracking for new nodes
        {
            let mut tracker = state.heartbeat_tracker.write().await;
            for mds in &mds_list {
                tracker.mds_failures.entry(mds.address.clone()).or_insert(0);
            }
            for ost in &ost_list {
                let key = format!("ost/{:08x}", ost.ost_index);
                tracker
                    .ost_failures
                    .entry(key)
                    .or_insert((ost.address.clone(), 0));
            }
        }

        // Send heartbeats to all MDS nodes concurrently
        let mds_futures: Vec<_> = mds_list
            .iter()
            .map(|mds| {
                let addr = mds.address.clone();
                async move {
                    let result = send_heartbeat(&addr).await;
                    (addr, result)
                }
            })
            .collect();

        // Send heartbeats to all OST nodes concurrently
        let ost_futures: Vec<_> = ost_list
            .iter()
            .map(|ost| {
                let addr = ost.address.clone();
                let key = format!("ost/{:08x}", ost.ost_index);
                async move {
                    let result = send_heartbeat(&addr).await;
                    (key, addr, result)
                }
            })
            .collect();

        // Wait for all heartbeat results
        let mds_results = futures::future::join_all(mds_futures).await;
        let ost_results = futures::future::join_all(ost_futures).await;

        // Process MDS heartbeat results
        let mut mds_to_remove = Vec::new();
        {
            let mut tracker = state.heartbeat_tracker.write().await;
            for (addr, result) in mds_results {
                if result {
                    // Success: reset failure count
                    tracker.mds_failures.insert(addr, 0);
                } else {
                    // Failure: increment failure count
                    let count = tracker.mds_failures.entry(addr.clone()).or_insert(0);
                    *count += 1;
                    warn!(
                        "MGS heartbeat: MDS {} failed ({}/{})",
                        addr, *count, MAX_HEARTBEAT_FAILURES
                    );
                    if *count >= MAX_HEARTBEAT_FAILURES {
                        mds_to_remove.push(addr);
                    }
                }
            }
        }

        // Process OST heartbeat results
        let mut ost_to_remove = Vec::new();
        {
            let mut tracker = state.heartbeat_tracker.write().await;
            for (key, addr, result) in ost_results {
                if result {
                    // Success: reset failure count
                    if let Some(entry) = tracker.ost_failures.get_mut(&key) {
                        entry.1 = 0;
                    }
                } else {
                    // Failure: increment failure count
                    let count = if let Some(entry) = tracker.ost_failures.get_mut(&key) {
                        entry.1 += 1;
                        entry.1
                    } else {
                        1
                    };
                    warn!(
                        "MGS heartbeat: OST {} ({}) failed ({}/{})",
                        key, addr, count, MAX_HEARTBEAT_FAILURES
                    );
                    if count >= MAX_HEARTBEAT_FAILURES {
                        ost_to_remove.push(key);
                    }
                }
            }
        }

        // Remove failed MDS nodes from FDB
        for addr in mds_to_remove {
            let key = format!("mds/{}", addr);
            if let Err(e) = state.store.delete(&key).await {
                error!("MGS heartbeat: failed to remove MDS {}: {e}", addr);
            } else {
                info!(
                    "MGS heartbeat: removed MDS {} after {} failed heartbeats",
                    addr, MAX_HEARTBEAT_FAILURES
                );
                let mut tracker = state.heartbeat_tracker.write().await;
                tracker.mds_failures.remove(&addr);
            }
        }

        // Remove failed OST nodes from FDB
        for key in ost_to_remove {
            if let Err(e) = state.store.delete(&key).await {
                error!("MGS heartbeat: failed to remove OST {}: {e}", key);
            } else {
                info!(
                    "MGS heartbeat: removed OST {} after {} failed heartbeats",
                    key, MAX_HEARTBEAT_FAILURES
                );
                let mut tracker = state.heartbeat_tracker.write().await;
                tracker.ost_failures.remove(&key);
            }
        }
    }
}

/// Send a heartbeat to a node and wait for reply with timeout.
/// Returns true if heartbeat succeeded, false otherwise.
async fn send_heartbeat(addr: &str) -> bool {
    let result = timeout(
        Duration::from_secs(HEARTBEAT_TIMEOUT_SECS),
        send_heartbeat_impl(addr),
    )
    .await;

    match result {
        Ok(Ok(())) => true,
        Ok(Err(e)) => {
            warn!("MGS heartbeat: error from {}: {e}", addr);
            false
        }
        Err(_) => {
            warn!("MGS heartbeat: timeout from {}", addr);
            false
        }
    }
}

/// Implementation of heartbeat send/receive.
async fn send_heartbeat_impl(addr: &str) -> Result<()> {
    let mut stream = TcpStream::connect(addr)
        .await
        .map_err(|e| RustreError::Net(format!("connect to {addr}: {e}")))?;

    // Send heartbeat message
    let msg = RpcMessage {
        id: 0, // Heartbeat doesn't need unique ID
        kind: RpcKind::Heartbeat,
    };
    let payload =
        bincode::serialize(&msg).map_err(|e| RustreError::Serialization(e.to_string()))?;
    let len = payload.len() as u32;
    stream
        .write_all(&len.to_be_bytes())
        .await
        .map_err(|e| RustreError::Net(e.to_string()))?;
    stream
        .write_all(&payload)
        .await
        .map_err(|e| RustreError::Net(e.to_string()))?;
    stream
        .flush()
        .await
        .map_err(|e| RustreError::Net(e.to_string()))?;

    // Read reply
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| RustreError::Net(format!("read length: {e}")))?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > 1024 {
        return Err(RustreError::Net(format!(
            "heartbeat reply too large: {len}"
        )));
    }
    let mut buf = vec![0u8; len];
    stream
        .read_exact(&mut buf)
        .await
        .map_err(|e| RustreError::Net(format!("read payload: {e}")))?;

    let reply: RpcMessage =
        bincode::deserialize(&buf).map_err(|e| RustreError::Serialization(e.to_string()))?;

    match reply.kind {
        RpcKind::HeartbeatReply => Ok(()),
        _ => Err(RustreError::Net("unexpected heartbeat reply".into())),
    }
}
