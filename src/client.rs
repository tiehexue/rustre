//! Rustre Client
//!
//! The client is analogous to Lustre's client module stack (LLite + LOV + OSC + MDC).
//! It:
//! 1. Fetches cluster config from MGS (learning MDS + OST addresses)
//! 2. Talks to MDS for metadata ops (create, lookup, readdir, unlink, stat)
//! 3. Talks directly to OSTs for data I/O — RAID-0 striped writes and parallel reads
//!
//! Object IDs and OST assignments are now computed deterministically from
//! (ino, stripe_seq) using StripeLayout helper methods — no need to store
//! per-object lists in metadata.

use crate::common::*;
use crate::net::rpc_call;
use clap::Subcommand;
use futures::future::join_all;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tracing::{error, info};

#[derive(Subcommand)]
pub enum ClientCommands {
    /// Write a local file into Rustre (striped across OSTs)
    Put {
        /// MGS address
        #[arg(short, long, default_value = "127.0.0.1:9400")]
        mgs: String,
        /// Local source file path
        source: String,
        /// Destination path in Rustre namespace
        dest: String,
        /// Stripe count (0 = all available OSTs)
        #[arg(short = 'c', long, default_value = "0")]
        stripe_count: u32,
        /// Stripe size in bytes
        #[arg(short = 'S', long, default_value = "1048576")]
        stripe_size: u64,
    },
    /// Read a file from Rustre to local disk (parallel fetch from OSTs)
    Get {
        /// MGS address
        #[arg(short, long, default_value = "127.0.0.1:9400")]
        mgs: String,
        /// Source path in Rustre namespace
        source: String,
        /// Local destination file path
        dest: String,
    },
    /// List a directory in Rustre
    Ls {
        /// MGS address
        #[arg(short, long, default_value = "127.0.0.1:9400")]
        mgs: String,
        /// Path in Rustre namespace
        #[arg(default_value = "/")]
        path: String,
    },
    /// Create a directory
    Mkdir {
        /// MGS address
        #[arg(short, long, default_value = "127.0.0.1:9400")]
        mgs: String,
        /// Path to create
        path: String,
    },
    /// Remove a file
    Rm {
        /// MGS address
        #[arg(short, long, default_value = "127.0.0.1:9400")]
        mgs: String,
        /// Path to remove
        path: String,
    },
    /// Stat a file (show metadata + stripe layout)
    Stat {
        /// MGS address
        #[arg(short, long, default_value = "127.0.0.1:9400")]
        mgs: String,
        /// Path in Rustre namespace
        path: String,
    },
}

/// Fetch cluster config from MGS.
async fn get_config(mgs_addr: &str) -> Result<ClusterConfig> {
    let reply = rpc_call(mgs_addr, RpcKind::GetConfig).await?;
    match reply.kind {
        RpcKind::ConfigReply(cfg) => Ok(cfg),
        RpcKind::Error(e) => Err(RustreError::Net(e)),
        _ => Err(RustreError::Net("unexpected reply".into())),
    }
}

/// Resolve MDS address from config (pick first MDS).
fn mds_addr(config: &ClusterConfig) -> Result<String> {
    config
        .mds_list
        .first()
        .map(|m| m.address.clone())
        .ok_or_else(|| RustreError::Net("no MDS registered in cluster".into()))
}

/// Look up an OST address by index.
fn ost_addr(config: &ClusterConfig, ost_index: u32) -> Result<String> {
    config
        .ost_list
        .iter()
        .find(|o| o.ost_index == ost_index)
        .map(|o| o.address.clone())
        .ok_or_else(|| RustreError::Net(format!("OST-{ost_index} not found in cluster")))
}

pub async fn run(cmd: ClientCommands) -> Result<()> {
    match cmd {
        ClientCommands::Put {
            mgs,
            source,
            dest,
            stripe_count,
            stripe_size,
        } => cmd_put(&mgs, &source, &dest, stripe_count, stripe_size).await,
        ClientCommands::Get { mgs, source, dest } => cmd_get(&mgs, &source, &dest).await,
        ClientCommands::Ls { mgs, path } => cmd_ls(&mgs, &path).await,
        ClientCommands::Mkdir { mgs, path } => cmd_mkdir(&mgs, &path).await,
        ClientCommands::Rm { mgs, path } => cmd_rm(&mgs, &path).await,
        ClientCommands::Stat { mgs, path } => cmd_stat(&mgs, &path).await,
    }
}

pub async fn status(mgs_addr: &str) -> Result<()> {
    let reply = rpc_call(mgs_addr, RpcKind::GetStatus).await?;
    match reply.kind {
        RpcKind::StatusReply(info) => {
            println!("╔══════════════════════════════════════════════╗");
            println!("║          Rustre Cluster Status               ║");
            println!("╠══════════════════════════════════════════════╣");
            println!("║  MDS servers: {:<31}║", info.mds_count);
            for mds in &info.mds_list {
                println!("║    └─ {:<39}║", mds.address);
            }
            println!("║  OST targets: {:<31}║", info.ost_count);
            for ost in &info.osts {
                let used_mb = ost.used_bytes / (1024 * 1024);
                let total_gb = ost.total_bytes / (1024 * 1024 * 1024);
                println!(
                    "║    └─ OST-{}: {} ({} MB / {} GB) ║",
                    ost.ost_index, ost.address, used_mb, total_gb
                );
            }
            println!("╚══════════════════════════════════════════════╝");
            Ok(())
        }
        RpcKind::Error(e) => {
            eprintln!("Error: {e}");
            Err(RustreError::Net(e))
        }
        _ => {
            eprintln!("Unexpected reply from MGS");
            Err(RustreError::Net("unexpected reply".into()))
        }
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
    let ost_count = config.ost_list.len() as u32;

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

    // Get the OST address for this assignment
    let ost_index = (layout.stripe_offset + ost_assignment) % ost_count;
    let addr = ost_addr(&config, ost_index)?;

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

        // Send to OSS
        let result = rpc_call(
            &addr,
            RpcKind::ObjWrite(ObjWriteReq {
                object_id: object_id.clone(),
                data: chunk_data,
            }),
        )
        .await;

        match result {
            Ok(reply) => match reply.kind {
                RpcKind::Ok => {
                    // Success, continue to next chunk for this OST
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
        "Created {dest} (ino={}, stripes={}, stripe_size={}, offset={})",
        meta.ino, layout.stripe_count, layout.stripe_size, layout.stripe_offset
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
    let ost_count = config.ost_list.len() as u32;

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
        let ost_index = (layout.stripe_offset + chunk_index) % ost_count;
        let addr = ost_addr(&config, ost_index)?;
        let tx = tx.clone();

        tokio::spawn(async move {
            let result = rpc_call(&addr, RpcKind::ObjRead(ObjReadReq { object_id })).await;
            let chunk_result = match result {
                Ok(reply) => match reply.kind {
                    RpcKind::DataReply(data) => Ok(data),
                    RpcKind::Error(e) => Err(RustreError::Net(e)),
                    _ => Err(RustreError::Net("unexpected OSS reply".into())),
                },
                Err(e) => Err(e),
            };

            // Send the result to the writer task
            if tx.send((chunk_index as usize, chunk_result)).await.is_err() {
                // Writer task has dropped, which means either success or fatal error
                error!("Writer task dropped while sending chunk {chunk_index}");
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
                let ost_count = config.ost_list.len() as u32;
                let total_chunks = layout.total_chunks(meta.size);
                println!("  Stripe Layout:");
                println!("    stripe_count:  {}", layout.stripe_count);
                println!("    stripe_size:   {} bytes", layout.stripe_size);
                println!("    stripe_offset: {}", layout.stripe_offset);
                println!("    total_chunks:  {}", total_chunks);
                println!("    Object mapping (deterministic):");
                for seq in 0..std::cmp::min(total_chunks, 16) {
                    let oid = StripeLayout::object_id(meta.ino, seq);
                    let ost = (layout.stripe_offset + seq) % ost_count;
                    println!("      [seq={seq}] object_id={oid} → OST-{ost}",);
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
