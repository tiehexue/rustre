//! Rustre Client
//!
//! The client is analogous to Lustre's client module stack (LLite + LOV + OSC + MDC).
//! It:
//! 1. Fetches cluster config from MGS (learning MDS + OST addresses)
//! 2. Talks to MDS for metadata ops (create, lookup, readdir, unlink, stat)
//! 3. Talks directly to OSTs for data I/O — RAID-0 striped writes and parallel reads
//!
//! Data flow for a PUT:
//!   Client reads local file → splits into stripe-sized chunks →
//!   sends each chunk to the appropriate OST in parallel →
//!   updates file size on MDS
//!
//! Data flow for a GET:
//!   Client fetches metadata (with stripe layout) from MDS →
//!   reads stripe objects from OSTs in parallel →
//!   reassembles into local file

use crate::common::*;
use crate::net::rpc_call;
use clap::Subcommand;
use futures::future::join_all;
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
                    ost.ost_index,
                    ost.address,
                    used_mb,
                    total_gb
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
// PUT — striped parallel write
// ---------------------------------------------------------------------------

async fn cmd_put(
    mgs_addr: &str,
    source: &str,
    dest: &str,
    stripe_count: u32,
    stripe_size: u64,
) -> Result<()> {
    // Read the local file
    let data = tokio::fs::read(source).await.map_err(|e| {
        RustreError::Io(std::io::Error::new(e.kind(), format!("reading {source}: {e}")))
    })?;
    let file_size = data.len() as u64;
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
        "Created {dest} (ino={}, stripes={}, stripe_size={})",
        meta.ino, layout.stripe_count, layout.stripe_size
    );

    // Split data into stripe-sized chunks and send to OSTs in parallel
    let chunk_size = layout.stripe_size as usize;
    let stripe_count = layout.stripe_count as usize;
    let mut write_futures = Vec::new();

    let mut offset = 0usize;
    let mut chunk_index = 0usize;
    while offset < data.len() {
        let end = std::cmp::min(offset + chunk_size, data.len());
        let chunk_data = data[offset..end].to_vec();
        let stripe_idx = chunk_index % stripe_count;
        let obj = &layout.objects[stripe_idx];
        let object_id = obj.object_id.clone();
        let ost_index = obj.ost_index;

        // Each stripe object accumulates data at increasing offsets
        // Offset within the object = (chunk_index / stripe_count) * chunk_size
        let obj_offset = (chunk_index / stripe_count) as u64 * layout.stripe_size;

        let addr = ost_addr(&config, ost_index)?;

        write_futures.push(tokio::spawn(async move {
            let result = rpc_call(
                &addr,
                RpcKind::ObjWrite(ObjWriteReq {
                    object_id: object_id.clone(),
                    offset: obj_offset,
                    data: chunk_data,
                }),
            )
            .await;
            match result {
                Ok(reply) => match reply.kind {
                    RpcKind::Ok => Ok(()),
                    RpcKind::Error(e) => Err(RustreError::Net(e)),
                    _ => Err(RustreError::Net("unexpected OSS reply".into())),
                },
                Err(e) => Err(e),
            }
        }));

        offset = end;
        chunk_index += 1;
    }

    // Wait for all writes to complete
    let results = join_all(write_futures).await;
    for (i, res) in results.into_iter().enumerate() {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                error!("Stripe write {i} failed: {e}");
                return Err(e);
            }
            Err(e) => {
                error!("Stripe write {i} panicked: {e}");
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
// GET — parallel stripe read
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
    let stripe_count = layout.stripe_count as usize;

    println!(
        "GET: {source} → {dest} ({} bytes, {} stripes)",
        file_size, stripe_count
    );

    // Calculate how many chunks total
    let total_chunks = (file_size + chunk_size - 1) / chunk_size;

    // Read all chunks in parallel
    let mut read_futures = Vec::new();
    for chunk_index in 0..total_chunks {
        let stripe_idx = chunk_index % stripe_count;
        let obj = &layout.objects[stripe_idx];
        let object_id = obj.object_id.clone();
        let ost_index = obj.ost_index;
        let obj_offset = (chunk_index / stripe_count) as u64 * layout.stripe_size;

        // How much to read from this chunk
        let remaining = file_size - chunk_index * chunk_size;
        let read_len = std::cmp::min(chunk_size, remaining) as u64;

        let addr = ost_addr(&config, ost_index)?;

        read_futures.push((
            chunk_index,
            tokio::spawn(async move {
                let result = rpc_call(
                    &addr,
                    RpcKind::ObjRead(ObjReadReq {
                        object_id,
                        offset: obj_offset,
                        length: read_len,
                    }),
                )
                .await;
                match result {
                    Ok(reply) => match reply.kind {
                        RpcKind::DataReply(data) => Ok(data),
                        RpcKind::Error(e) => Err(RustreError::Net(e)),
                        _ => Err(RustreError::Net("unexpected OSS reply".into())),
                    },
                    Err(e) => Err(e),
                }
            }),
        ));
    }

    // Collect results in order
    let mut file_data = vec![0u8; file_size];
    for (chunk_index, future) in read_futures {
        let data = future
            .await
            .map_err(|e| RustreError::Internal(format!("read task panicked: {e}")))?
            .map_err(|e| {
                error!("Stripe read chunk {chunk_index} failed: {e}");
                e
            })?;

        let file_offset = chunk_index * chunk_size;
        let copy_len = std::cmp::min(data.len(), file_size - file_offset);
        file_data[file_offset..file_offset + copy_len].copy_from_slice(&data[..copy_len]);
    }

    // Write to local file
    tokio::fs::write(dest, &file_data).await?;
    println!("Successfully read {file_size} bytes from {source} to {dest}");
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
                println!("{:<8} {:<6} {:>12} {}", "INO", "TYPE", "SIZE", "NAME");
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
// RM — remove file
// ---------------------------------------------------------------------------

async fn cmd_rm(mgs_addr: &str, path: &str) -> Result<()> {
    let config = get_config(mgs_addr).await?;
    let mds = mds_addr(&config)?;

    // First get the file metadata to know which objects to delete
    let stat_reply = rpc_call(&mds, RpcKind::Stat(path.to_string())).await?;
    let meta = match stat_reply.kind {
        RpcKind::MetaReply(m) => m,
        RpcKind::Error(e) => return Err(RustreError::Net(e)),
        _ => return Err(RustreError::Net("unexpected reply".into())),
    };

    // Delete stripe objects from OSTs
    if let Some(layout) = &meta.layout {
        let mut delete_futures = Vec::new();
        for obj in &layout.objects {
            let addr = ost_addr(&config, obj.ost_index)?;
            let oid = obj.object_id.clone();
            delete_futures.push(tokio::spawn(async move {
                let _ = rpc_call(&addr, RpcKind::ObjDelete { object_id: oid }).await;
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
            println!("  Type:    {}", if meta.is_dir { "directory" } else { "file" });
            println!("  Size:    {} bytes", meta.size);
            println!("  Created: {}", meta.ctime);
            println!("  Modified:{}", meta.mtime);
            if let Some(layout) = &meta.layout {
                println!("  Stripe Layout:");
                println!("    stripe_count:  {}", layout.stripe_count);
                println!("    stripe_size:   {} bytes", layout.stripe_size);
                println!("    stripe_offset: {}", layout.stripe_offset);
                println!("    Objects:");
                for (i, obj) in layout.objects.iter().enumerate() {
                    println!(
                        "      [{i}] object_id={} ost_index={} offset={} length={}",
                        obj.object_id, obj.ost_index, obj.offset, obj.length
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
