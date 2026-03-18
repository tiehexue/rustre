//! Client operations implementation

use crate::client::commands::ClientCommands;
use crate::error::{Result, RustreError};
use crate::rpc::{rpc_call, RpcKind};
use crate::types::{ClusterConfig, CreateReq, ObjReadReq, ObjWriteReq, StripeLayout};
use futures::future::join_all;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tracing::{debug, error, info};

/// Fetch cluster config from MGS.
pub async fn get_config(mgs_addr: &str) -> Result<ClusterConfig> {
    let reply = rpc_call(mgs_addr, RpcKind::GetConfig).await?;
    match reply.kind {
        RpcKind::ConfigReply(cfg) => Ok(cfg),
        RpcKind::Error(e) => Err(RustreError::Net(e)),
        _ => Err(RustreError::Net("unexpected reply".into())),
    }
}

/// Resolve MDS address from config (pick first MDS).
pub fn mds_addr(config: &ClusterConfig) -> Result<String> {
    config
        .mds_list
        .first()
        .map(|m| m.address.clone())
        .ok_or_else(|| RustreError::Net("no MDS registered in cluster".into()))
}

/// Look up an OST address by index.
pub fn ost_addr(config: &ClusterConfig, ost_index: u32) -> Result<String> {
    config
        .ost_list
        .iter()
        .find(|o| o.ost_index == ost_index)
        .map(|o| o.address.clone())
        .ok_or_else(|| RustreError::Net(format!("OST-{ost_index} not found in cluster")))
}

/// Main entry point for client operations
pub async fn run(cmd: ClientCommands) -> Result<()> {
    match cmd {
        ClientCommands::Put {
            mgs,
            source,
            dest,
            stripe_count,
            stripe_size,
            replica_count,
        } => {
            cmd_put(
                &mgs,
                &source,
                &dest,
                stripe_count,
                stripe_size,
                replica_count,
            )
            .await
        }
        ClientCommands::Get { mgs, source, dest } => cmd_get(&mgs, &source, &dest).await,
        ClientCommands::Ls { mgs, path } => cmd_ls(&mgs, &path).await,
        ClientCommands::Mkdir { mgs, path } => cmd_mkdir(&mgs, &path).await,
        ClientCommands::Rm { mgs, path } => cmd_rm(&mgs, &path).await,
        ClientCommands::Stat { mgs, path } => cmd_stat(&mgs, &path).await,
    }
}

// ---------------------------------------------------------------------------
// PUT — striped parallel write (using deterministic object IDs)
// ---------------------------------------------------------------------------

/// Helper function for a single OST writer task
async fn ost_writer_task(
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
            RpcKind::ObjWrite(ObjWriteReq {
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
                            rpc_call(&addr, RpcKind::ObjWrite(ObjWriteReq { object_id, data }))
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

async fn cmd_put(
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

        write_futures.push(tokio::spawn(async move {
            ost_writer_task(
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

// ---------------------------------------------------------------------------
// GET — parallel stripe read with streaming writes (using deterministic object IDs)
// ---------------------------------------------------------------------------

async fn cmd_get(mgs_addr: &str, source: &str, dest: &str) -> Result<()> {
    let config = get_config(mgs_addr).await?;
    let mds = mds_addr(&config)?;

    // Stat the file to get metadata + stripe layout
    let reply = rpc_call(&mds, RpcKind::Stat(source.to_string())).await?;
    let meta = match reply.kind {
        RpcKind::MetaReply(m) => m,
        RpcKind::Error(e) => return Err(RustreError::Net(e)),
        _ => return Err(RustreError::Net("unexpected reply from MDS".into())),
    };

    if meta.is_dir {
        return Err(RustreError::IsDirectory(source.to_string()));
    }

    let layout = meta
        .layout
        .as_ref()
        .ok_or_else(|| RustreError::Internal("no stripe layout".into()))?;

    let file_size = meta.size as usize;
    let chunk_size = layout.stripe_size as usize;
    println!(
        "GET: {source} → {dest} ({} bytes, {} stripes, streaming)",
        file_size, layout.stripe_count
    );

    // Calculate how many chunks total
    let total_chunks = layout.total_chunks(meta.size);

    // Create a channel for streaming chunks from readers to writer
    // Use a bounded channel to control memory usage (buffer up to 4 chunks)
    let (tx, mut rx) = tokio::sync::mpsc::channel::<(usize, Result<Vec<u8>>)>(4);

    // Spawn all reader tasks in parallel
    for chunk_index in 0..total_chunks {
        let object_id = StripeLayout::object_id(meta.ino, chunk_index);
        let primary_ost_index = layout.ost_for_chunk(chunk_index);
        let tx = tx.clone();
        let config = config.clone();
        let layout = layout.clone();

        tokio::spawn(async move {
            // Try primary OST first
            let mut addrs_to_try = Vec::new();

            // Add primary address
            if let Ok(primary_addr) = ost_addr(&config, primary_ost_index) {
                addrs_to_try.push(primary_addr);
            }

            // Add replica addresses if available
            if layout.replica_count > 1 && !layout.replica_map.is_empty() {
                // Find which index in ost_indices corresponds to this primary_ost_index
                let ost_indices = if layout.ost_indices.is_empty() {
                    // If ost_indices is empty, we're using round-robin
                    let pos = (primary_ost_index as i64 - layout.stripe_offset as i64)
                        .rem_euclid(layout.stripe_count as i64)
                        as usize;
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
                        for &replica_ost_index in &layout.replica_map[pos] {
                            if let Ok(addr) = ost_addr(&config, replica_ost_index) {
                                addrs_to_try.push(addr);
                            }
                        }
                    }
                }
            }

            let mut last_error = None;

            // Try each address in order (primary first, then replicas)
            for addr in addrs_to_try {
                let result = rpc_call(
                    &addr,
                    RpcKind::ObjRead(ObjReadReq {
                        object_id: object_id.clone(),
                    }),
                )
                .await;
                match result {
                    Ok(reply) => match reply.kind {
                        RpcKind::DataReply(data) => {
                            // Success! Send the data and return
                            if tx.send((chunk_index as usize, Ok(data))).await.is_err() {
                                error!("Writer task dropped while sending chunk {chunk_index}");
                            }
                            return;
                        }
                        RpcKind::Error(e) => {
                            last_error = Some(RustreError::Net(e));
                            // Continue to next replica
                            debug!("trying next replica!");
                            continue;
                        }
                        _ => {
                            last_error = Some(RustreError::Net("unexpected OSS reply".into()));
                            continue;
                        }
                    },
                    Err(e) => {
                        last_error = Some(e);
                        continue;
                    }
                }
            }

            // If we get here, all attempts failed
            let error =
                last_error.unwrap_or_else(|| RustreError::Net("no OST addresses available".into()));
            if tx.send((chunk_index as usize, Err(error))).await.is_err() {
                error!("Writer task dropped while sending error for chunk {chunk_index}");
            }
        });
    }

    // Drop our copy of the sender so the channel closes when all readers finish
    drop(tx);

    // Create or truncate the destination file
    let mut file = tokio::fs::File::create(dest).await.map_err(|e| {
        RustreError::Io(std::io::Error::new(
            e.kind(),
            format!("creating destination file {dest}: {e}"),
        ))
    })?;

    // Track which chunks we've received and need to write
    let mut next_chunk_to_write = 0;
    let mut pending_chunks = std::collections::BTreeMap::new();

    // Process chunks as they arrive, writing them in order
    while let Some((chunk_index, chunk_result)) = rx.recv().await {
        match chunk_result {
            Ok(data) => {
                // Store the chunk in pending map
                pending_chunks.insert(chunk_index, data);

                // Write as many consecutive chunks as we have in order
                while let Some(data) = pending_chunks.remove(&next_chunk_to_write) {
                    let file_offset = (next_chunk_to_write * chunk_size) as u64;
                    let copy_len =
                        std::cmp::min(data.len(), file_size - (next_chunk_to_write * chunk_size));

                    // Seek to the correct position and write the chunk
                    file.seek(std::io::SeekFrom::Start(file_offset))
                        .await
                        .map_err(|e| {
                            RustreError::Io(std::io::Error::new(
                                e.kind(),
                                format!("seeking to offset {file_offset} in {dest}: {e}"),
                            ))
                        })?;

                    file.write_all(&data[..copy_len]).await.map_err(|e| {
                        RustreError::Io(std::io::Error::new(
                            e.kind(),
                            format!("writing chunk {next_chunk_to_write} to {dest}: {e}"),
                        ))
                    })?;

                    next_chunk_to_write += 1;

                    // If we've written all chunks, we're done
                    if next_chunk_to_write >= total_chunks as usize {
                        break;
                    }
                }
            }
            Err(e) => {
                error!("Stripe read chunk {chunk_index} failed: {e}");
                return Err(e);
            }
        }
    }

    // Verify we wrote all chunks
    if next_chunk_to_write < total_chunks as usize {
        return Err(RustreError::Internal(format!(
            "only wrote {}/{} chunks before channel closed",
            next_chunk_to_write, total_chunks
        )));
    }

    // Ensure all data is flushed to disk
    file.sync_all().await.map_err(|e| {
        RustreError::Io(std::io::Error::new(
            e.kind(),
            format!("syncing file {dest}: {e}"),
        ))
    })?;

    println!("Successfully read {file_size} bytes from {source} to {dest} (streaming)");
    Ok(())
}

// ---------------------------------------------------------------------------
// LS — list directory
// ---------------------------------------------------------------------------

async fn cmd_ls(mgs_addr: &str, path: &str) -> Result<()> {
    let config = get_config(mgs_addr).await?;
    let mds = mds_addr(&config)?;

    let reply = rpc_call(&mds, RpcKind::Readdir(path.to_string())).await?;
    match reply.kind {
        RpcKind::MetaListReply(entries) => {
            if entries.is_empty() {
                println!("(empty directory)");
            } else {
                println!("{:<8} {:<6} {:>12} NAME", "INO", "TYPE", "SIZE");
                println!("{}", "-".repeat(50));
                for entry in entries {
                    let kind = if entry.is_dir { "dir" } else { "file" };
                    let stripes = entry
                        .layout
                        .as_ref()
                        .map(|l| format!(" [{}x{}]", l.stripe_count, l.stripe_size))
                        .unwrap_or_default();
                    println!(
                        "{:<8} {:<6} {:>12} {}{}",
                        entry.ino, kind, entry.size, entry.name, stripes
                    );
                }
            }
            Ok(())
        }
        RpcKind::Error(e) => {
            eprintln!("Error: {e}");
            Err(RustreError::Net(e))
        }
        _ => Err(RustreError::Net("unexpected reply".into())),
    }
}

// ---------------------------------------------------------------------------
// MKDIR
// ---------------------------------------------------------------------------

async fn cmd_mkdir(mgs_addr: &str, path: &str) -> Result<()> {
    let config = get_config(mgs_addr).await?;
    let mds = mds_addr(&config)?;

    let reply = rpc_call(&mds, RpcKind::Mkdir(path.to_string())).await?;
    match reply.kind {
        RpcKind::Ok => {
            println!("Created directory: {path}");
            Ok(())
        }
        RpcKind::Error(e) => {
            eprintln!("Error: {e}");
            Err(RustreError::Net(e))
        }
        _ => Err(RustreError::Net("unexpected reply".into())),
    }
}

// ---------------------------------------------------------------------------
// RM — remove file (uses bulk ObjDeleteInode for efficient cleanup)
// ---------------------------------------------------------------------------

async fn cmd_rm(mgs_addr: &str, path: &str) -> Result<()> {
    let config = get_config(mgs_addr).await?;
    let mds = mds_addr(&config)?;

    // First get the file metadata to know which OSTs to notify
    let stat_reply = rpc_call(&mds, RpcKind::Stat(path.to_string())).await?;
    let meta = match stat_reply.kind {
        RpcKind::MetaReply(m) => m,
        RpcKind::Error(e) => return Err(RustreError::Net(e)),
        _ => return Err(RustreError::Net("unexpected reply".into())),
    };

    // Delete all stripe objects from OSTs using bulk inode deletion.
    // We send ObjDeleteInode to all OSTs that could hold stripes for this file.
    if meta.layout.is_some() {
        // Send bulk delete to all OSTs in the cluster (they'll handle prefix scan)
        let mut delete_futures = Vec::new();
        for ost_info in &config.ost_list {
            let addr = ost_info.address.clone();
            let ino = meta.ino;
            delete_futures.push(tokio::spawn(async move {
                let _ = rpc_call(&addr, RpcKind::ObjDeleteInode { ino }).await;
            }));
        }
        join_all(delete_futures).await;
    }

    // Remove from MDS
    let reply = rpc_call(&mds, RpcKind::Unlink(path.to_string())).await?;
    match reply.kind {
        RpcKind::Ok => {
            println!("Removed: {path}");
            Ok(())
        }
        RpcKind::Error(e) => {
            eprintln!("Error: {e}");
            Err(RustreError::Net(e))
        }
        _ => Err(RustreError::Net("unexpected reply".into())),
    }
}

// ---------------------------------------------------------------------------
// STAT — file info + stripe layout
// ---------------------------------------------------------------------------

async fn cmd_stat(mgs_addr: &str, path: &str) -> Result<()> {
    let config = get_config(mgs_addr).await?;
    let mds = mds_addr(&config)?;

    let reply = rpc_call(&mds, RpcKind::Stat(path.to_string())).await?;
    match reply.kind {
        RpcKind::MetaReply(meta) => {
            println!("  Path:    {}", meta.path);
            println!("  Inode:   {}", meta.ino);
            println!(
                "  Type:    {}",
                if meta.is_dir { "directory" } else { "file" }
            );
            println!("  Size:    {} bytes", meta.size);
            println!("  Created: {}", meta.ctime);
            println!("  Modified:{}", meta.mtime);
            if let Some(layout) = &meta.layout {
                let total_chunks = layout.total_chunks(meta.size);
                println!("  Stripe Layout:");
                println!("    stripe_count:  {}", layout.stripe_count);
                println!("    stripe_size:   {} bytes", layout.stripe_size);
                println!("    stripe_offset: {}", layout.stripe_offset);
                println!("    replica_count: {}", layout.replica_count);
                println!("    total_chunks:  {}", total_chunks);
                println!("    Object mapping (deterministic):");
                for seq in 0..std::cmp::min(total_chunks, 16) {
                    let oid = StripeLayout::object_id(meta.ino, seq);
                    let ost = layout.ost_for_chunk(seq);

                    // Get replica information
                    let replica_info = if layout.replica_count > 1 && !layout.replica_map.is_empty()
                    {
                        // Find which index in ost_indices corresponds to this ost
                        let ost_indices = if layout.ost_indices.is_empty() {
                            // If ost_indices is empty, we're using round-robin
                            let pos = (ost as i64 - layout.stripe_offset as i64)
                                .rem_euclid(layout.stripe_count as i64)
                                as usize;
                            if pos < layout.stripe_count as usize {
                                Some(pos)
                            } else {
                                None
                            }
                        } else {
                            layout.ost_indices.iter().position(|&idx| idx == ost)
                        };

                        if let Some(pos) = ost_indices {
                            if pos < layout.replica_map.len() {
                                let replicas = &layout.replica_map[pos];
                                if !replicas.is_empty() {
                                    format!(
                                        " (replicas: {})",
                                        replicas
                                            .iter()
                                            .map(|r| r.to_string())
                                            .collect::<Vec<_>>()
                                            .join(", ")
                                    )
                                } else {
                                    String::new()
                                }
                            } else {
                                String::new()
                            }
                        } else {
                            String::new()
                        }
                    } else {
                        String::new()
                    };

                    println!(
                        "      [seq={seq}] object_id={oid} → OST-{}{}",
                        ost, replica_info
                    );
                }
                if total_chunks > 16 {
                    println!("      ... ({} more chunks)", total_chunks - 16);
                }
            }
            Ok(())
        }
        RpcKind::Error(e) => {
            eprintln!("Error: {e}");
            Err(RustreError::Net(e))
        }
        _ => Err(RustreError::Net("unexpected reply".into())),
    }
}
