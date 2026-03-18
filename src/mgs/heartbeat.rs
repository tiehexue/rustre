//! Heartbeat management for MGS

use crate::error::{Result, RustreError};
use crate::rpc::RpcKind;
use crate::storage::FdbMetaStore;
use crate::types::{MdsInfo, OstInfo};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::time::timeout;
use tracing::{error, info, warn};

/// Heartbeat timeout in seconds
const HEARTBEAT_TIMEOUT_SECS: u64 = 1;
/// Maximum consecutive heartbeat failures before removing a node
const MAX_HEARTBEAT_FAILURES: u32 = 2;

/// Tracks heartbeat failure counts for registered nodes
#[derive(Debug, Default)]
pub struct HeartbeatTracker {
    /// MDS address -> consecutive failure count
    pub mds_failures: HashMap<String, u32>,
    /// OST key (ost/{index}) -> (address, consecutive failure count)
    pub ost_failures: HashMap<String, (String, u32)>,
}

/// Send a heartbeat to a node and wait for reply with timeout.
/// Returns true if heartbeat succeeded, false otherwise.
pub async fn send_heartbeat(addr: &str) -> bool {
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
    let msg = crate::rpc::RpcMessage {
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

    let reply: crate::rpc::RpcMessage =
        bincode::deserialize(&buf).map_err(|e| RustreError::Serialization(e.to_string()))?;

    match reply.kind {
        RpcKind::HeartbeatReply => Ok(()),
        _ => Err(RustreError::Net("unexpected heartbeat reply".into())),
    }
}

/// Background task that sends heartbeats to all registered MDS/OSS every second.
/// If a node fails to respond within 1 second for 2 consecutive attempts, it is removed.
pub async fn heartbeat_scan_loop(
    store: Arc<FdbMetaStore>,
    heartbeat_tracker: Arc<RwLock<HeartbeatTracker>>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));

    loop {
        interval.tick().await;

        // Get current list of registered MDS and OST from FDB
        let mds_list: Vec<MdsInfo> = match store.list_prefix("mds/").await {
            Ok(list) => list,
            Err(e) => {
                warn!("MGS heartbeat: failed to list MDS: {e}");
                continue;
            }
        };

        let ost_list: Vec<OstInfo> = match store.list_prefix("ost/").await {
            Ok(list) => list,
            Err(e) => {
                warn!("MGS heartbeat: failed to list OST: {e}");
                continue;
            }
        };

        // Initialize tracking for new nodes
        {
            let mut tracker = heartbeat_tracker.write().await;
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
            let mut tracker = heartbeat_tracker.write().await;
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
            let mut tracker = heartbeat_tracker.write().await;
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
            if let Err(e) = store.delete(&key).await {
                error!("MGS heartbeat: failed to remove MDS {}: {e}", addr);
            } else {
                info!(
                    "MGS heartbeat: removed MDS {} after {} failed heartbeats",
                    addr, MAX_HEARTBEAT_FAILURES
                );
                let mut tracker = heartbeat_tracker.write().await;
                tracker.mds_failures.remove(&addr);
            }
        }

        // Remove failed OST nodes from FDB
        for key in ost_to_remove {
            if let Err(e) = store.delete(&key).await {
                error!("MGS heartbeat: failed to remove OST {}: {e}", key);
            } else {
                info!(
                    "MGS heartbeat: removed OST {} after {} failed heartbeats",
                    key, MAX_HEARTBEAT_FAILURES
                );
                let mut tracker = heartbeat_tracker.write().await;
                tracker.ost_failures.remove(&key);
            }
        }
    }
}
