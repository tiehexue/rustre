//! Rustre — A parallel, distributed file system inspired by Lustre.
//!
//! Architecture overview:
//!   MGS    — Management Server: cluster config in FoundationDB (stateless, HA)
//!   MDS    — Metadata Server: namespace ops via FoundationDB (stateless, HA)
//!   OSS    — Object Storage Server: file data on local filesystem (zero-copy)
//!   Client — CLI that talks to MDS for metadata + OSS for data I/O

mod client;
mod error;
mod mds;
mod mgs;
#[cfg(feature = "fuse")]
mod mount;
mod oss;
mod rpc;
mod storage;
mod types;
mod utils;
mod zerocopy;

use clap::{Parser, Subcommand};
use client::commands::ClientCommands;

/// Rustre — a parallel distributed file system
#[derive(Parser)]
#[command(
    name = "rustre",
    version,
    about = "A parallel distributed file system inspired by Lustre"
)]
struct Cli {
    /// Log level (info, debug, trace, warn, error)
    #[arg(long, global = true, default_value = "info")]
    log_level: String,

    /// MGS address to register with, ignored in mgs instance
    #[arg(short, long, global = true, default_value = "127.0.0.1:9400")]
    mgs: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the Management Server (MGS) — backed by FoundationDB for stateless HA
    Mgs {
        /// Listen address
        #[arg(short, long, default_value = "0.0.0.0:9400")]
        listen: String,
        /// Cluster name (used as FoundationDB key prefix)
        #[arg(short, long, default_value = "rustre")]
        cluster_name: String,
    },
    /// Start a Metadata Server (MDS) — backed by FoundationDB for stateless HA
    Mds {
        /// Listen address
        #[arg(short, long, default_value = "0.0.0.0:9401")]
        listen: String,
        /// Cluster name (used as FoundationDB key prefix, must match MGS)
        #[arg(short, long, default_value = "rustre")]
        cluster_name: String,
    },
    /// Start an Object Storage Server (OSS) — file-backed, zero-copy enabled
    Oss {
        /// Listen address
        #[arg(short, long, default_value = "0.0.0.0:9402")]
        listen: String,
        /// Data directory for object storage
        #[arg(short, long, default_value = "/tmp/rustre/oss")]
        data_dir: String,
        /// OST index (unique per OSS instance)
        #[arg(short = 'i', long, default_value = "0")]
        ost_index: u32,
    },
    /// Client operations — interact with the Rustre filesystem
    #[command(subcommand)]
    Client(ClientCommands),
    /// Print cluster status from MGS
    Status {
        /// MGS address
        #[arg(short, long, default_value = "127.0.0.1:9400")]
        mgs: String,
    },
    /// Mount the Rustre filesystem via FUSE (macOS/Linux)
    #[cfg(feature = "fuse")]
    Mount {
        /// Local mountpoint directory
        mountpoint: String,
    },
}

fn main() -> anyhow::Result<()> {
    // Parse CLI arguments first
    let cli = Cli::parse();

    // Initialize logging based on the command and log level
    utils::logging::init_logging(&cli.command, &cli.log_level)?;

    // FUSE mount creates its own tokio runtime internally (fuser callbacks are sync,
    // RustreFs bridges to async via a dedicated runtime). We must NOT enter a tokio
    // runtime before calling mount, otherwise we get "Cannot start a runtime from
    // within a runtime". So we handle the Mount command before building the async rt.
    #[cfg(feature = "fuse")]
    if let Commands::Mount { ref mountpoint } = cli.command {
        return mount::mount(&cli.mgs, mountpoint).map_err(Into::into);
    }

    // All other commands are async — spin up the tokio runtime
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async_main(cli))
}

async fn async_main(cli: Cli) -> anyhow::Result<()> {
    let mgs = &cli.mgs;

    match cli.command {
        Commands::Mgs {
            listen: _listen,
            cluster_name: _cluster_name,
        } => {
            // Initialize FoundationDB network (must happen once per process lifetime)
            // SAFETY: We call drop(network) at process exit via the tokio runtime shutdown.
            #[cfg(feature = "fdb")]
            {
                let _network = unsafe { foundationdb::boot() };
                mgs::run(&_listen, &_cluster_name).await?;
            }
            #[cfg(not(feature = "fdb"))]
            return Err(anyhow::anyhow!("MGS command requires FoundationDB support. Build with --features fdb or use default features."));
        }
        Commands::Mds {
            listen: _listen,
            cluster_name: _cluster_name,
        } => {
            // MDS also uses FoundationDB — boot the network
            #[cfg(feature = "fdb")]
            {
                let _network = unsafe { foundationdb::boot() };
                mds::run(&_listen, mgs, &_cluster_name).await?;
            }
            #[cfg(not(feature = "fdb"))]
            return Err(anyhow::anyhow!("MDS command requires FoundationDB support. Build with --features fdb or use default features."));
        }
        Commands::Oss {
            listen,
            data_dir,
            ost_index,
        } => {
            oss::run(&listen, mgs, &data_dir, ost_index).await?;
        }
        Commands::Client(cmd) => {
            client::run(cmd, mgs).await?;
        }
        Commands::Status { mgs } => {
            client::status(&mgs).await?;
        }
        #[cfg(feature = "fuse")]
        Commands::Mount { .. } => {
            // Already handled in main() before the async runtime was created
            unreachable!("Mount command is handled before async runtime");
        }
    }

    Ok(())
}
