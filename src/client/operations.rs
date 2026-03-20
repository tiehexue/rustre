//! Client operations implementation

use crate::client::commands::ClientCommands;
use crate::client::put::cmd_put;
use crate::client::rm::cmd_rm;
use crate::error::{Result, RustreError};
use crate::rpc::{recv_msg, rpc_call, send_msg, RpcKind, RpcMessage, MSG_COUNTER};
use crate::types::{ClusterConfig, StripeLayout};
use std::sync::atomic::Ordering;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, error};

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
        "GET: {source} → {dest} ({} bytes, {} stripes, zero-copy)",
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
                // Connect to OSS for zero-copy read
                let mut stream = match TcpStream::connect(&addr).await {
                    Ok(stream) => stream,
                    Err(e) => {
                        last_error = Some(RustreError::Net(format!("connect to {addr}: {e}")));
                        continue;
                    }
                };

                // Send zero-copy read request
                let request_msg = RpcMessage {
                    id: MSG_COUNTER.fetch_add(1, Ordering::Relaxed),
                    kind: RpcKind::ObjReadZeroCopy {
                        object_id: object_id.clone(),
                        length: 0, // 0 means read entire file
                    },
                };

                if let Err(e) = send_msg(&mut stream, &request_msg).await {
                    last_error = Some(e);
                    continue;
                }

                // Receive reply with length
                let reply = match recv_msg(&mut stream).await {
                    Ok(reply) => reply,
                    Err(e) => {
                        last_error = Some(e);
                        continue;
                    }
                };

                match reply.kind {
                    RpcKind::DataReply(length_bytes) => {
                        // Parse the length from the reply
                        if length_bytes.len() != 8 {
                            last_error = Some(RustreError::Net("invalid length reply".into()));
                            continue;
                        }
                        let length = usize::from_le_bytes([
                            length_bytes[0],
                            length_bytes[1],
                            length_bytes[2],
                            length_bytes[3],
                            length_bytes[4],
                            length_bytes[5],
                            length_bytes[6],
                            length_bytes[7],
                        ]);

                        // Read the data from socket (zero-copy data follows)
                        let mut data = vec![0u8; length];
                        if let Err(e) = stream.read_exact(&mut data).await {
                            last_error = Some(RustreError::Net(format!("read data: {e}")));
                            continue;
                        }
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

    println!("Successfully read {file_size} bytes from {source} to {dest} (zero-copy)");
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

pub async fn cmd_mkdir(mgs_addr: &str, path: &str) -> Result<()> {
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
