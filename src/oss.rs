//! Object Storage Server (OSS) / Object Storage Target (OST)
//!
//! The OSS stores actual file data as objects in RocksDB. Each file is striped
//! across multiple OSTs (RAID-0 style). The OSS:
//! - Registers its OST with the MGS on startup
//! - Handles ObjWrite / ObjRead / ObjDelete / ObjDeleteInode RPCs from clients
//! - Periodically reports disk usage back to the MGS
//! - Persists objects in RocksDB via RocksObjectStore

use crate::common::*;
use crate::net::{make_reply, recv_msg, rpc_call, send_msg};
use crate::storage::RocksObjectStore;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};

pub async fn run(listen: &str, mgs_addr: &str, data_dir: &str, ost_index: u32) -> Result<()> {
    let store = RocksObjectStore::new(data_dir)?;
    let store = Arc::new(store);

    // Register with MGS
    register_with_mgs(mgs_addr, listen, ost_index).await?;

    // Spawn background usage reporter
    {
        let store = Arc::clone(&store);
        let mgs = mgs_addr.to_string();
        let idx = ost_index;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(15)).await;
                if let Ok(used) = store.total_usage().await {
                    let _ = rpc_call(
                        &mgs,
                        RpcKind::UpdateOstUsage {
                            ost_index: idx,
                            used_bytes: used,
                        },
                    )
                    .await;
                }
            }
        });
    }

    let listener = TcpListener::bind(listen).await?;
    info!("OSS (OST-{ost_index}) listening on {listen} (RocksDB-backed)");

    loop {
        let (stream, addr) = listener.accept().await?;
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, store).await {
                warn!("OSS: error handling connection from {addr}: {e}");
            }
        });
    }
}

async fn register_with_mgs(mgs_addr: &str, listen: &str, ost_index: u32) -> Result<()> {
    let addr = if listen.starts_with("0.0.0.0") {
        listen.replace("0.0.0.0", "127.0.0.1")
    } else {
        listen.to_string()
    };
    let ost_info = OstInfo {
        ost_index,
        address: addr.clone(),
        total_bytes: 1_000_000_000_000, // 1 TB nominal capacity
        used_bytes: 0,
    };
    let reply = rpc_call(mgs_addr, RpcKind::RegisterOst(ost_info)).await?;
    match reply.kind {
        RpcKind::Ok => {
            info!("OSS: OST-{ost_index} registered with MGS as {addr}");
            Ok(())
        }
        RpcKind::Error(e) => Err(RustreError::Net(e)),
        _ => Err(RustreError::Net("unexpected reply from MGS".into())),
    }
}

async fn handle_connection(mut stream: TcpStream, store: Arc<RocksObjectStore>) -> Result<()> {
    let msg = recv_msg(&mut stream).await?;
    let reply = match msg.kind {
        RpcKind::ObjWrite(req) => match store.write(&req.object_id, &req.data).await {
            Ok(()) => make_reply(msg.id, RpcKind::Ok),
            Err(e) => make_reply(msg.id, RpcKind::Error(e.to_string())),
        },

        RpcKind::ObjRead(req) => match store.read(&req.object_id).await {
            Ok(data) => make_reply(msg.id, RpcKind::DataReply(data)),
            Err(e) => make_reply(msg.id, RpcKind::Error(e.to_string())),
        },

        RpcKind::ObjDelete { object_id } => match store.delete(&object_id).await {
            Ok(()) => make_reply(msg.id, RpcKind::Ok),
            Err(e) => make_reply(msg.id, RpcKind::Error(e.to_string())),
        },

        RpcKind::ObjDeleteInode { ino } => match store.delete_inode(ino).await {
            Ok(()) => make_reply(msg.id, RpcKind::Ok),
            Err(e) => make_reply(msg.id, RpcKind::Error(e.to_string())),
        },

        RpcKind::Heartbeat => {
            // Respond to heartbeat immediately
            make_reply(msg.id, RpcKind::HeartbeatReply)
        }

        other => {
            warn!("OSS: unexpected RPC: {other:?}");
            make_reply(
                msg.id,
                RpcKind::Error("unsupported operation for OSS".into()),
            )
        }
    };
    send_msg(&mut stream, &reply).await
}
