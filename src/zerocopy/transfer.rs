//! Zero-copy transfer implementation for Rustre

use crate::client::operations::ost_addr;
use crate::error::{Result, RustreError};
use crate::rpc::{recv_msg, send_msg, RpcKind, MSG_COUNTER};
use crate::types::{ClusterConfig, StripeLayout};
use tokio::net::TcpStream;
use tracing::debug;

/// Send one chunk to a primary OST (and its replicas) via zero-copy.
async fn zerocopy_transfer(
    source_path: &str,
    primary_addr: &str,
    replica_addrs: &[String],
    object_id: &str,
    file_offset: u64,
    actual_chunk_size: usize,
) -> Result<()> {
    // Platform-specific setup
    #[cfg(unix)]
    use std::os::fd::AsRawFd;

    #[cfg(windows)]
    use std::sync::Arc;

    #[cfg(windows)]
    use std::os::windows::io::AsRawSocket;

    use crate::zerocopy::send_file;

    // Open the source file (platform-specific)
    #[cfg(unix)]
    let file = std::fs::File::open(source_path).map_err(|e| {
        RustreError::Io(std::io::Error::new(
            e.kind(),
            format!("opening {source_path} for OST zero-copy writer: {e}"),
        ))
    })?;

    #[cfg(windows)]
    let (file, file_descriptor) = {
        let file = std::fs::File::open(source_path).map_err(|e| {
            RustreError::Io(std::io::Error::new(
                e.kind(),
                format!("opening {source_path} for OST zero-copy writer: {e}"),
            ))
        })?;
        let file = Arc::new(file);
        (Arc::clone(&file), file)
    };

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

    // Get socket descriptor (platform-specific)
    #[cfg(unix)]
    let socket_descriptor = primary_stream.as_raw_fd();

    #[cfg(windows)]
    let socket_descriptor = primary_stream.as_raw_socket();

    // Perform zero-copy transfer (spawn_blocking to avoid blocking async runtime)
    let sendfile_result = {
        #[cfg(unix)]
        let file_clone = file.try_clone().map_err(|e| {
            RustreError::Io(std::io::Error::new(
                e.kind(),
                format!("cloning file handle for primary transfer: {e}"),
            ))
        })?;
        #[cfg(windows)]
        let file_ref = file_descriptor;

        tokio::task::spawn_blocking(move || {
            #[cfg(unix)]
            return send_file(
                file_clone.as_raw_fd(),
                socket_descriptor,
                file_offset,
                actual_chunk_size,
            );

            #[cfg(windows)]
            return send_file(&file_ref, socket_descriptor, file_offset, actual_chunk_size);
        })
        .await
        .map_err(|e| RustreError::Internal(format!("zero-copy task panicked: {}", e)))?
    };

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
                "zero-copy transfer failed: {}",
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

                #[cfg(unix)]
                let file_clone = file.try_clone().map_err(|e| {
                    RustreError::Io(std::io::Error::new(
                        e.kind(),
                        format!("cloning file handle for replica transfer: {e}"),
                    ))
                })?;
                #[cfg(windows)]
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

                    // Get socket descriptor for replica (platform-specific)
                    #[cfg(unix)]
                    let socket_fd = replica_stream.as_raw_fd();
                    #[cfg(windows)]
                    let socket = replica_stream.as_raw_socket();

                    // Perform zero-copy transfer to replica (spawn_blocking)
                    let sendfile_result = tokio::task::spawn_blocking(move || {
                        #[cfg(unix)]
                        return send_file(
                            file_clone.as_raw_fd(),
                            socket_fd,
                            file_offset,
                            actual_chunk_size,
                        );

                        #[cfg(windows)]
                        return send_file(&file_clone, socket, file_offset, actual_chunk_size);
                    })
                    .await
                    .map_err(|e| {
                        RustreError::Internal(format!("zero-copy to replica task panicked: {}", e))
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
                                "zero-copy to replica failed: {}",
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

/// Per-OST writer task: sends all chunks assigned to one OST via zero-copy.
pub async fn ost_zerocopy_task(
    source_path: String,
    meta_ino: u64,
    layout: StripeLayout,
    config: ClusterConfig,
    ost_assignment: u32, // Which OST this task is responsible for (0..ost_indices.len()-1)
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

    // Collect replica OST addresses (empty when replication is disabled)
    let replica_addrs: Vec<String> = layout
        .replica_ost_indices(primary_ost_index)
        .into_iter()
        .filter_map(|idx| ost_addr(&config, idx).ok())
        .collect();

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

        zerocopy_transfer(
            &source_path,
            &primary_addr,
            &replica_addrs,
            &object_id,
            file_offset,
            actual_chunk_size,
        )
        .await?;

        // Advance to next chunk for this OST (skip by ost_indices.len())
        chunk_index += layout.ost_indices.len() as u32;
    }

    Ok(())
}
