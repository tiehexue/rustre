//! Object Storage Server (OSS) / Object Storage Target (OST)
//!
//! The OSS stores actual file data as objects in files on disk. Each file is striped
//! across multiple OSTs (RAID-0 style). The OSS:
//! - Registers its OST with the MGS on startup
//! - Handles ObjWrite / ObjRead / ObjDelete / ObjDeleteInode RPCs from clients
//! - Handles zero-copy operations: ObjWriteZeroCopy / ObjReadZeroCopy
//! - Periodically reports disk usage back to the MGS
//! - Persists objects as files via FileObjectStore (enables zero-copy)

use crate::error::{Result, RustreError};
use crate::rpc::{make_reply, recv_msg, rpc_call, send_msg, RpcKind};
use crate::storage::FileObjectStore;
use crate::types::OstInfo;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

#[cfg(unix)]
use std::os::fd::AsRawFd;

#[cfg(windows)]
use std::os::windows::io::AsRawSocket;

pub async fn run(listen: &str, mgs_addr: &str, data_dir: &str, ost_index: u32) -> Result<()> {
    let store = FileObjectStore::new(data_dir)?;
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
    info!("OSS (OST-{ost_index}) listening on {listen} (FileStore-backed, zero-copy enabled)");

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

/// Helper function to perform zero-copy transfer
async fn zerocopy_send(
    file: std::fs::File,
    stream: &TcpStream,
    bytes_to_send: usize,
) -> Result<usize> {
    #[cfg(unix)]
    {
        let socket_fd = stream.as_raw_fd();
        let file_fd = file.as_raw_fd();

        tokio::task::spawn_blocking(move || {
            crate::zerocopy::send_file(file_fd, socket_fd, 0, bytes_to_send)
        })
        .await
        .map_err(|e| RustreError::Internal(format!("spawn_blocking: {e}")))?
    }

    #[cfg(windows)]
    {
        let socket = stream.as_raw_socket();

        tokio::task::spawn_blocking(move || {
            crate::zerocopy::send_file(&file, socket, 0, bytes_to_send)
        })
        .await
        .map_err(|e| RustreError::Internal(format!("spawn_blocking: {e}")))?
    }
}

async fn handle_connection(mut stream: TcpStream, store: Arc<FileObjectStore>) -> Result<()> {
    let msg = recv_msg(&mut stream).await?;
    let reply = match msg.kind {
        RpcKind::ObjWriteZeroCopy { object_id, length } => {
            // Read the data that was sent via sendfile after the RPC message
            let mut data = vec![0u8; length];
            if let Err(e) = stream.read_exact(&mut data).await {
                // Failed to read data, send error reply
                make_reply(
                    msg.id,
                    RpcKind::Error(format!("failed to read zero-copy data: {}", e)),
                )
            } else {
                debug!(
                    "OSS: received zero-copy write for object {object_id} ({} bytes)",
                    length
                );
                match store.write(&object_id, &data).await {
                    Ok(()) => make_reply(msg.id, RpcKind::Ok),
                    Err(e) => make_reply(msg.id, RpcKind::Error(e.to_string())),
                }
            }
        }
        RpcKind::ObjReadZeroCopy { object_id, length } => {
            // Zero-copy read - open file and send via sendfile
            match store.open_for_zerocopy(&object_id) {
                Ok((file, file_len)) => {
                    // Determine how many bytes to send (use file length if length is 0 or larger than file)
                    let bytes_to_send = if length == 0 || length > file_len as usize {
                        file_len as usize
                    } else {
                        length
                    };

                    // First send OK reply with the actual length
                    let ok_reply = make_reply(
                        msg.id,
                        RpcKind::DataReply(bytes_to_send.to_le_bytes().to_vec()),
                    );
                    send_msg(&mut stream, &ok_reply).await?;

                    // Perform zero-copy transfer
                    match zerocopy_send(file, &stream, bytes_to_send).await {
                        Ok(sent) => {
                            debug!(
                                "OSS: zero-copy sent {} bytes for object {}",
                                sent, object_id
                            );
                            // No reply needed after sendfile, we already sent the header
                            return Ok(());
                        }
                        Err(e) => {
                            warn!("OSS: zero-copy read failed for {}: {}", object_id, e);
                            // Connection is likely in bad state, just return error
                            return Err(e);
                        }
                    }
                }
                Err(e) => make_reply(msg.id, RpcKind::Error(e.to_string())),
            }
        }

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
