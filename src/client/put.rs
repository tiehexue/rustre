//! PUT command implementation for Rustre client

use crate::client::operations::{get_config, mds_addr, ost_addr};
use crate::error::{Result, RustreError};
use crate::rpc::{rpc_call, RpcKind};
use crate::types::{ClusterConfig, CreateReq, StripeLayout};
use futures::future::join_all;
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tracing::{debug, error, info};

/// Helper function for a single OST writer task
#[allow(dead_code)]
pub async fn ost_writer_task(
    source_path: String,
    meta_ino: u64,
    layout: StripeLayout,
    config: ClusterConfig,
    ost_assignment: u32, // Which OST this task is responsible for (0..stripe_count-1)
) -> Result<()> {
    let chunk_size = layout.stripe_size as usize;
    // Open the source file for this task
    let mut file = tokio::fs::File::open(&source_path).await.map_err(|e| {
        RustreError::Io(std::io::Error::new(
            e.kind(),
            format!("opening {source_path} for OST writer: {e}"),
        ))
    })?;

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

        // Seek to the correct position in the file
        file.seek(std::io::SeekFrom::Start(file_offset))
            .await
            .map_err(|e| {
                RustreError::Io(std::io::Error::new(
                    e.kind(),
                    format!("seeking to offset {file_offset} in {source_path}: {e}"),
                ))
            })?;

        // Read the chunk - must read exactly chunk_size bytes or until EOF
        let mut buffer = vec![0u8; chunk_size];
        let mut total_read = 0;
        while total_read < chunk_size {
            let bytes_read = file.read(&mut buffer[total_read..]).await.map_err(|e| {
                RustreError::Io(std::io::Error::new(
                    e.kind(),
                    format!("reading chunk {chunk_index} from {source_path}: {e}"),
                ))
            })?;

            if bytes_read == 0 {
                // EOF reached
                break;
            }
            total_read += bytes_read;
        }

        if total_read == 0 {
            // EOF reached at start (shouldn't happen if we calculated total_chunks correctly)
            break;
        }

        // Trim buffer to actual bytes read
        buffer.truncate(total_read);
        let chunk_data = buffer;

        // Deterministic object ID
        let object_id = StripeLayout::object_id(meta_ino, chunk_index);

        // Write to primary OSS
        let primary_result = rpc_call(
            &primary_addr,
            RpcKind::ObjWrite(crate::types::ObjWriteReq {
                object_id: object_id.clone(),
                data: chunk_data.clone(),
            }),
        )
        .await;

        match primary_result {
            Ok(reply) => match reply.kind {
                RpcKind::Ok => {
                    // Primary write successful, now write to replicas
                    let mut replica_futures = Vec::new();
                    for replica_addr in &replica_addrs {
                        let addr = replica_addr.clone();
                        let object_id = object_id.clone();
                        let data = chunk_data.clone();
                        replica_futures.push(tokio::spawn(async move {
                            rpc_call(
                                &addr,
                                RpcKind::ObjWrite(crate::types::ObjWriteReq { object_id, data }),
                            )
                            .await
                        }));
                    }

                    // Wait for all replica writes to complete
                    let replica_results = futures::future::join_all(replica_futures).await;
                    for (i, result) in replica_results.into_iter().enumerate() {
                        match result {
                            Ok(Ok(reply)) => match reply.kind {
                                RpcKind::Ok => {
                                    // Replica write successful
                                    debug!("replica {} write successfully", i);
                                }
                                RpcKind::Error(e) => {
                                    return Err(RustreError::Net(format!(
                                        "replica {} write failed: {}",
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
                                    "replica {} write error: {}",
                                    i, e
                                )));
                            }
                            Err(e) => {
                                return Err(RustreError::Internal(format!(
                                    "replica {} task panicked: {}",
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
            },
            Err(e) => {
                return Err(e);
            }
        }

        // Move to next chunk for this OST (skip by stripe_count)
        chunk_index += layout.stripe_count;
    }

    Ok(())
}

/// PUT command implementation
pub async fn cmd_put(
    mgs_addr: &str,
    source: &str,
    dest: &str,
    stripe_count: u32,
    stripe_size: u64,
    replica_count: u32,
) -> Result<()> {
    let metadata = tokio::fs::metadata(source).await.map_err(|e| {
        RustreError::Io(std::io::Error::new(
            e.kind(),
            format!("getting metadata for {source}: {e}"),
        ))
    })?;

    if metadata.is_dir() {
        put_directory(
            mgs_addr,
            source,
            dest,
            stripe_count,
            stripe_size,
            replica_count,
        )
        .await
    } else {
        put_file(
            mgs_addr,
            source,
            dest,
            stripe_count,
            stripe_size,
            replica_count,
        )
        .await
    }
}

async fn put_file(
    mgs_addr: &str,
    source: &str,
    dest: &str,
    stripe_count: u32,
    stripe_size: u64,
    replica_count: u32,
) -> Result<()> {
    // Get file size first
    let file_size = tokio::fs::metadata(source)
        .await
        .map_err(|e| {
            RustreError::Io(std::io::Error::new(
                e.kind(),
                format!("getting metadata for {source}: {e}"),
            ))
        })?
        .len();
    info!("PUT: {source} → {dest} ({file_size} bytes)");

    // Fetch cluster config
    let config = get_config(mgs_addr).await?;
    let mds = mds_addr(&config)?;

    // Create file on MDS (gets stripe layout back)
    let reply = rpc_call(
        &mds,
        RpcKind::Create(CreateReq {
            path: dest.to_string(),
            stripe_count,
            stripe_size,
            replica_count,
        }),
    )
    .await?;

    let meta = match reply.kind {
        RpcKind::MetaReply(m) => m,
        RpcKind::Error(e) => return Err(RustreError::Net(e)),
        _ => return Err(RustreError::Net("unexpected reply from MDS".into())),
    };

    let layout = meta
        .layout
        .as_ref()
        .ok_or_else(|| RustreError::Internal("no stripe layout returned".into()))?;

    println!(
        "Created {dest} (ino={}, stripes={}, stripe_size={}, offset={}, replica_count={})",
        meta.ino,
        layout.stripe_count,
        layout.stripe_size,
        layout.stripe_offset,
        layout.replica_count
    );

    // Create one task per stripe (layout.stripe_count tasks)
    let mut write_futures = Vec::new();
    for ost_assignment in 0..layout.stripe_count {
        let source_path = source.to_string();
        let config_clone = config.clone();
        let layout_clone = layout.clone();
        let meta_ino = meta.ino;

        #[cfg(target_os = "macos")]
        write_futures.push(tokio::spawn(async move {
            crate::zerocopy::transfer::ost_zerocopy_task(
                source_path,
                meta_ino,
                layout_clone,
                config_clone,
                ost_assignment,
            )
            .await
        }));

        #[cfg(target_os = "windows")]
        write_futures.push(tokio::spawn(async move {
            crate::zerocopy::transfer::ost_zerocopy_task(
                source_path,
                meta_ino,
                layout_clone,
                config_clone,
                ost_assignment,
            )
            .await
        }));
    }

    // Wait for all writes to complete
    let results = join_all(write_futures).await;
    for (i, res) in results.into_iter().enumerate() {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                error!("OST writer task {i} failed: {e}");
                return Err(e);
            }
            Err(e) => {
                error!("OST writer task {i} panicked: {e}");
                return Err(RustreError::Internal(format!("write task panicked: {e}")));
            }
        }
    }

    // Update file size on MDS
    let _ = rpc_call(
        &mds,
        RpcKind::SetSize {
            path: dest.to_string(),
            size: file_size,
        },
    )
    .await?;

    println!(
        "Successfully wrote {file_size} bytes to {dest} across {} OSTs",
        layout.stripe_count
    );
    Ok(())
}

async fn put_directory(
    mgs_addr: &str,
    source: &str,
    dest: &str,
    stripe_count: u32,
    stripe_size: u64,
    replica_count: u32,
) -> Result<()> {
    use crate::client::operations::cmd_mkdir;

    let source_path = Path::new(source);

    async fn walk_and_put(
        mgs_addr: &str,
        source_root: &Path,
        dest_root: &str,
        current_source: &Path,
        stripe_count: u32,
        stripe_size: u64,
        replica_count: u32,
    ) -> Result<()> {
        let mut entries = tokio::fs::read_dir(current_source).await.map_err(|e| {
            RustreError::Io(std::io::Error::new(
                e.kind(),
                format!("reading directory {}: {e}", current_source.display()),
            ))
        })?;

        while let Some(entry) = entries.next_entry().await.map_err(|e| {
            RustreError::Io(std::io::Error::new(
                e.kind(),
                format!(
                    "reading directory entry in {}: {e}",
                    current_source.display()
                ),
            ))
        })? {
            let entry_path = entry.path();
            let relative_path = entry_path.strip_prefix(source_root).map_err(|_| {
                RustreError::Internal(format!(
                    "failed to compute relative path for {}",
                    entry_path.display()
                ))
            })?;
            let dest_entry_path = Path::new(dest_root).join(relative_path);

            let metadata = tokio::fs::metadata(&entry_path).await.map_err(|e| {
                RustreError::Io(std::io::Error::new(
                    e.kind(),
                    format!("getting metadata for {}: {e}", entry_path.display()),
                ))
            })?;

            if metadata.is_dir() {
                // Create directory on remote
                cmd_mkdir(mgs_addr, &dest_entry_path.to_string_lossy()).await?;
                // Recurse
                Box::pin(walk_and_put(
                    mgs_addr,
                    source_root,
                    dest_root,
                    &entry_path,
                    stripe_count,
                    stripe_size,
                    replica_count,
                ))
                .await?;
            } else {
                // Put file
                put_file(
                    mgs_addr,
                    &entry_path.to_string_lossy(),
                    &dest_entry_path.to_string_lossy(),
                    stripe_count,
                    stripe_size,
                    replica_count,
                )
                .await?;
            }
        }
        Ok(())
    }

    // Create the root destination directory if it doesn't exist
    cmd_mkdir(mgs_addr, dest).await?;
    // Walk and put
    walk_and_put(
        mgs_addr,
        source_path,
        dest,
        source_path,
        stripe_count,
        stripe_size,
        replica_count,
    )
    .await
}
