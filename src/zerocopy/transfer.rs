//! Zero-copy transfer implementation for Rustre

use crate::client::operations::ost_addr;
use crate::error::{Result, RustreError};
use crate::rpc::{recv_msg, send_msg, RpcKind, MSG_COUNTER};
use crate::types::{ClusterConfig, StripeLayout};
use tokio::net::TcpStream;
use tracing::debug;

/// Unified zero-copy OST writer task that works on both macOS and Windows
pub async fn ost_zerocopy_task(
    source_path: String,
    meta_ino: u64,
    layout: StripeLayout,
    config: ClusterConfig,
    ost_assignment: u32, // Which OST this task is responsible for (0..stripe_count-1)
) -> Result<()> {
    let chunk_size = layout.stripe_size as usize;

    // Calculate total chunks needed
    let file_size = tokio::fs::metadata(&source_path)
        .await
        .map_err(|e| {
            RustreError::Io(std::io::Error::new(
                e.kind(),
                format!("getting metadata for {source_path}: {e}"),
            ))
        })?
        .len();
    let total_chunks = layout.total_chunks(file_size);

    // Get the primary OST address for this assignment using the layout's mapping
    let primary_ost_index = layout.ost_for_chunk(ost_assignment);
    let primary_addr = ost_addr(&config, primary_ost_index)?;

    // Get replica OST addresses if replica_count > 1
    let replica_addrs = if layout.replica_count > 1 && !layout.replica_map.is_empty() {
        // Find which index in ost_indices corresponds to this primary_ost_index
        let ost_indices = if layout.ost_indices.is_empty() {
            // If ost_indices is empty, we're using round-robin
            // We need to calculate which position this primary_ost_index is in
            let pos = (primary_ost_index as i64 - layout.stripe_offset as i64)
                .rem_euclid(layout.stripe_count as i64) as usize;
            if pos < layout.stripe_count as usize {
                Some(pos)
            } else {
                None
            }
        } else {
            layout
                .ost_indices
                .iter()
                .position(|&idx| idx == primary_ost_index)
        };

        if let Some(pos) = ost_indices {
            if pos < layout.replica_map.len() {
                let mut addrs = Vec::new();
                for &replica_ost_index in &layout.replica_map[pos] {
                    if let Ok(addr) = ost_addr(&config, replica_ost_index) {
                        addrs.push(addr);
                    }
                }
                addrs
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    // Process all chunks that belong to this OST
    let mut chunk_index = ost_assignment; // Start with the first chunk for this OST
    while chunk_index < total_chunks {
        // Calculate file offset for this chunk
        let file_offset = (chunk_index as u64) * (chunk_size as u64);

        // Calculate actual chunk size (may be smaller at EOF)
        let remaining_file_size = file_size.saturating_sub(file_offset);
        let actual_chunk_size = std::cmp::min(chunk_size as u64, remaining_file_size) as usize;

        if actual_chunk_size == 0 {
            // EOF reached (shouldn't happen if we calculated total_chunks correctly)
            break;
        }

        // Deterministic object ID
        let object_id = StripeLayout::object_id(meta_ino, chunk_index);

        // Platform-specific zero-copy implementation
        #[cfg(target_os = "macos")]
        let result = macos_zerocopy_transfer(
            &source_path,
            &primary_addr,
            &replica_addrs,
            &object_id,
            file_offset,
            actual_chunk_size,
        )
        .await;

        #[cfg(target_os = "windows")]
        let result = windows_zerocopy_transfer(
            &source_path,
            &primary_addr,
            &replica_addrs,
            &object_id,
            file_offset,
            actual_chunk_size,
        )
        .await;

        match result {
            Ok(_) => {
                // Move to next chunk for this OST (skip by stripe_count)
                chunk_index += layout.stripe_count;
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    Ok(())
}

/// macOS-specific zero-copy transfer using sendfile
#[cfg(target_os = "macos")]
async fn macos_zerocopy_transfer(
    source_path: &str,
    primary_addr: &str,
    replica_addrs: &[String],
    object_id: &str,
    file_offset: u64,
    actual_chunk_size: usize,
) -> Result<()> {
    use crate::zerocopy::send_file;
    use std::os::fd::AsRawFd;

    // Open the source file for this task using std::fs to get RawFd
    let file = std::fs::File::open(source_path).map_err(|e| {
        RustreError::Io(std::io::Error::new(
            e.kind(),
            format!("opening {source_path} for OST zero-copy writer: {e}"),
        ))
    })?;
    let file_fd = file.as_raw_fd();

    // Connect to primary OSS
    let mut primary_stream = TcpStream::connect(primary_addr)
        .await
        .map_err(|e| RustreError::Net(format!("connect to {primary_addr}: {e}")))?;

    // Send zero-copy write request to primary
    let request_msg = crate::rpc::RpcMessage {
        id: MSG_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        kind: RpcKind::ObjWriteZeroCopy {
            object_id: object_id.to_string(),
            length: actual_chunk_size,
        },
    };

    send_msg(&mut primary_stream, &request_msg).await?;

    // Get socket file descriptor for sendfile
    let socket_fd = primary_stream.as_raw_fd();

    // Use sendfile for zero-copy transfer (spawn_blocking to avoid blocking async runtime)
    let sendfile_result = tokio::task::spawn_blocking(move || {
        send_file(file_fd, socket_fd, file_offset, actual_chunk_size)
    })
    .await
    .map_err(|e| RustreError::Internal(format!("sendfile task panicked: {}", e)))?;

    match sendfile_result {
        Ok(bytes_sent) => {
            if bytes_sent != actual_chunk_size {
                return Err(RustreError::ZeroCopyError(format!(
                    "partial transfer: sent {}/{} bytes",
                    bytes_sent, actual_chunk_size
                )));
            }
        }
        Err(e) => {
            return Err(RustreError::ZeroCopyError(format!(
                "sendfile failed: {}",
                e
            )));
        }
    }

    // Receive reply from primary
    let primary_reply = recv_msg(&mut primary_stream).await?;
    match primary_reply.kind {
        RpcKind::Ok => {
            // Primary write successful, now write to replicas
            let mut replica_futures = Vec::new();
            for replica_addr in replica_addrs {
                let addr = replica_addr.clone();
                let object_id = object_id.to_string();
                let file_fd = file.as_raw_fd();

                replica_futures.push(tokio::spawn(async move {
                    // Connect to replica
                    let mut replica_stream = TcpStream::connect(&addr)
                        .await
                        .map_err(|e| RustreError::Net(format!("connect to {addr}: {e}")))?;

                    // Send zero-copy write request to replica
                    let request_msg = crate::rpc::RpcMessage {
                        id: MSG_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                        kind: RpcKind::ObjWriteZeroCopy {
                            object_id: object_id.clone(),
                            length: actual_chunk_size,
                        },
                    };

                    send_msg(&mut replica_stream, &request_msg).await?;

                    // Use sendfile for zero-copy transfer to replica (spawn_blocking)
                    let socket_fd = replica_stream.as_raw_fd();
                    let sendfile_result = tokio::task::spawn_blocking(move || {
                        send_file(file_fd, socket_fd, file_offset, actual_chunk_size)
                    })
                    .await
                    .map_err(|e| {
                        RustreError::Internal(format!("sendfile to replica task panicked: {}", e))
                    })?;

                    match sendfile_result {
                        Ok(bytes_sent) => {
                            if bytes_sent != actual_chunk_size {
                                return Err(RustreError::ZeroCopyError(format!(
                                    "partial transfer to replica: sent {}/{} bytes",
                                    bytes_sent, actual_chunk_size
                                )));
                            }
                        }
                        Err(e) => {
                            return Err(RustreError::ZeroCopyError(format!(
                                "sendfile to replica failed: {}",
                                e
                            )));
                        }
                    }

                    // Receive reply from replica
                    let reply = recv_msg(&mut replica_stream).await?;
                    Ok(reply)
                }));
            }

            // Wait for all replica writes to complete
            let replica_results = futures::future::join_all(replica_futures).await;
            for (i, result) in replica_results.into_iter().enumerate() {
                match result {
                    Ok(Ok(reply)) => match reply.kind {
                        RpcKind::Ok => {
                            // Replica write successful
                            debug!("replica {} zero-copy write successfully", i);
                        }
                        RpcKind::Error(e) => {
                            return Err(RustreError::Net(format!(
                                "replica {} zero-copy write failed: {}",
                                i, e
                            )));
                        }
                        _ => {
                            return Err(RustreError::Net(format!(
                                "unexpected reply from replica {}",
                                i
                            )));
                        }
                    },
                    Ok(Err(e)) => {
                        return Err(RustreError::Net(format!(
                            "replica {} zero-copy write error: {}",
                            i, e
                        )));
                    }
                    Err(e) => {
                        return Err(RustreError::Internal(format!(
                            "replica {} zero-copy task panicked: {}",
                            i, e
                        )));
                    }
                }
            }
        }
        RpcKind::Error(e) => {
            return Err(RustreError::Net(e));
        }
        _ => {
            return Err(RustreError::Net("unexpected OSS reply".into()));
        }
    }

    Ok(())
}

/// Windows-specific zero-copy transfer using TransmitFile
#[cfg(target_os = "windows")]
async fn windows_zerocopy_transfer(
    source_path: &str,
    primary_addr: &str,
    replica_addrs: &[String],
    object_id: &str,
    file_offset: u64,
    actual_chunk_size: usize,
) -> Result<()> {
    use crate::zerocopy::send_file;
    use std::os::windows::io::AsRawSocket;
    use std::sync::Arc;

    // Open the source file for this task using std::fs to get File
    let file = std::fs::File::open(source_path).map_err(|e| {
        RustreError::Io(std::io::Error::new(
            e.kind(),
            format!("opening {source_path} for OST zero-copy writer: {e}"),
        ))
    })?;

    // Wrap the file in Arc to share it between closures
    let file = Arc::new(file);

    // Connect to primary OSS
    let mut primary_stream = TcpStream::connect(primary_addr)
        .await
        .map_err(|e| RustreError::Net(format!("connect to {primary_addr}: {e}")))?;

    // Send zero-copy write request to primary
    let request_msg = crate::rpc::RpcMessage {
        id: MSG_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        kind: RpcKind::ObjWriteZeroCopy {
            object_id: object_id.to_string(),
            length: actual_chunk_size,
        },
    };

    send_msg(&mut primary_stream, &request_msg).await?;

    // Get socket handle for TransmitFile
    let socket = primary_stream.as_raw_socket();

    // Clone the Arc for the closure
    let file_clone = Arc::clone(&file);

    // Use TransmitFile for zero-copy transfer (spawn_blocking to avoid blocking async runtime)
    let sendfile_result = tokio::task::spawn_blocking(move || {
        send_file(&file_clone, socket, file_offset, actual_chunk_size)
    })
    .await
    .map_err(|e| RustreError::Internal(format!("TransmitFile task panicked: {}", e)))?;

    match sendfile_result {
        Ok(bytes_sent) => {
            if bytes_sent != actual_chunk_size {
                return Err(RustreError::ZeroCopyError(format!(
                    "partial transfer: sent {}/{} bytes",
                    bytes_sent, actual_chunk_size
                )));
            }
        }
        Err(e) => {
            return Err(RustreError::ZeroCopyError(format!(
                "TransmitFile failed: {}",
                e
            )));
        }
    }

    // Receive reply from primary
    let primary_reply = recv_msg(&mut primary_stream).await?;
    match primary_reply.kind {
        RpcKind::Ok => {
            // Primary write successful, now write to replicas
            let mut replica_futures = Vec::new();
            for replica_addr in replica_addrs {
                let addr = replica_addr.clone();
                let object_id = object_id.to_string();
                let file_clone = Arc::clone(&file);

                replica_futures.push(tokio::spawn(async move {
                    // Connect to replica
                    let mut replica_stream = TcpStream::connect(&addr)
                        .await
                        .map_err(|e| RustreError::Net(format!("connect to {addr}: {e}")))?;

                    // Send zero-copy write request to replica
                    let request_msg = crate::rpc::RpcMessage {
                        id: MSG_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                        kind: RpcKind::ObjWriteZeroCopy {
                            object_id: object_id.clone(),
                            length: actual_chunk_size,
                        },
                    };

                    send_msg(&mut replica_stream, &request_msg).await?;

                    // Use TransmitFile for zero-copy transfer to replica (spawn_blocking)
                    let socket = replica_stream.as_raw_socket();
                    let sendfile_result = tokio::task::spawn_blocking(move || {
                        send_file(&file_clone, socket, file_offset, actual_chunk_size)
                    })
                    .await
                    .map_err(|e| {
                        RustreError::Internal(format!(
                            "TransmitFile to replica task panicked: {}",
                            e
                        ))
                    })?;

                    match sendfile_result {
                        Ok(bytes_sent) => {
                            if bytes_sent != actual_chunk_size {
                                return Err(RustreError::ZeroCopyError(format!(
                                    "partial transfer to replica: sent {}/{} bytes",
                                    bytes_sent, actual_chunk_size
                                )));
                            }
                        }
                        Err(e) => {
                            return Err(RustreError::ZeroCopyError(format!(
                                "TransmitFile to replica failed: {}",
                                e
                            )));
                        }
                    }

                    // Receive reply from replica
                    let reply = recv_msg(&mut replica_stream).await?;
                    Ok(reply)
                }));
            }

            // Wait for all replica writes to complete
            let replica_results = futures::future::join_all(replica_futures).await;
            for (i, result) in replica_results.into_iter().enumerate() {
                match result {
                    Ok(Ok(reply)) => match reply.kind {
                        RpcKind::Ok => {
                            // Replica write successful
                            debug!("replica {} zero-copy write successfully", i);
                        }
                        RpcKind::Error(e) => {
                            return Err(RustreError::Net(format!(
                                "replica {} zero-copy write failed: {}",
                                i, e
                            )));
                        }
                        _ => {
                            return Err(RustreError::Net(format!(
                                "unexpected reply from replica {}",
                                i
                            )));
                        }
                    },
                    Ok(Err(e)) => {
                        return Err(RustreError::Net(format!(
                            "replica {} zero-copy write error: {}",
                            i, e
                        )));
                    }
                    Err(e) => {
                        return Err(RustreError::Internal(format!(
                            "replica {} zero-copy task panicked: {}",
                            i, e
                        )));
                    }
                }
            }
        }
        RpcKind::Error(e) => {
            return Err(RustreError::Net(e));
        }
        _ => {
            return Err(RustreError::Net("unexpected OSS reply".into()));
        }
    }

    Ok(())
}
