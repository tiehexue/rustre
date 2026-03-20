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
        /// MGS address to register with
        #[arg(short, long, default_value = "127.0.0.1:9400")]
        mgs: String,
        /// Cluster name (used as FoundationDB key prefix, must match MGS)
        #[arg(short, long, default_value = "rustre")]
        cluster_name: String,
    },
    /// Start an Object Storage Server (OSS) — file-backed, zero-copy enabled
    Oss {
        /// Listen address
        #[arg(short, long, default_value = "0.0.0.0:9402")]
        listen: String,
        /// MGS address to register with
        #[arg(short, long, default_value = "127.0.0.1:9400")]
        mgs: String,
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse CLI arguments first
    let cli = Cli::parse();

    // Initialize logging based on the command and log level
    utils::logging::init_logging(&cli.command, &cli.log_level)?;

    match cli.command {
        Commands::Mgs {
            listen,
            cluster_name,
        } => {
            // Initialize FoundationDB network (must happen once per process lifetime)
            // SAFETY: We call drop(network) at process exit via the tokio runtime shutdown.
            let _network = unsafe { foundationdb::boot() };
            mgs::run(&listen, &cluster_name).await?;
        }
        Commands::Mds {
            listen,
            mgs,
            cluster_name,
        } => {
            // MDS also uses FoundationDB — boot the network
            let _network = unsafe { foundationdb::boot() };
            mds::run(&listen, &mgs, &cluster_name).await?;
        }
        Commands::Oss {
            listen,
            mgs,
            data_dir,
            ost_index,
        } => {
            oss::run(&listen, &mgs, &data_dir, ost_index).await?;
        }
        Commands::Client(cmd) => {
            client::run(cmd).await?;
        }
        Commands::Status { mgs } => {
            client::status(&mgs).await?;
        }
    }

    Ok(())
}
