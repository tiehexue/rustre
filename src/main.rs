//! Rustre — A parallel, distributed file system inspired by Lustre.
//!
//! Architecture overview:
//!   MGS  — Management Server: stores cluster configuration, distributes it to peers
//!   MDS  — Metadata Server: handles namespace ops (open/stat/mkdir/readdir/unlink)
//!   OSS  — Object Storage Server: stores actual file data in striped objects
//!   Client — POSIX-ish CLI that talks to MDS+OSS for file I/O

mod common;
mod mgs;
mod mds;
mod oss;
mod client;
mod net;
mod storage;

use clap::{Parser, Subcommand};
use client::ClientCommands;

/// Rustre — a parallel distributed file system
#[derive(Parser)]
#[command(name = "rustre", version, about = "A parallel distributed file system inspired by Lustre")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the Management Server (MGS)
    Mgs {
        /// Listen address
        #[arg(short, long, default_value = "0.0.0.0:9400")]
        listen: String,
        /// Data directory for MGS state
        #[arg(short, long, default_value = "/tmp/rustre/mgs")]
        data_dir: String,
    },
    /// Start a Metadata Server (MDS)
    Mds {
        /// Listen address
        #[arg(short, long, default_value = "0.0.0.0:9401")]
        listen: String,
        /// MGS address to register with
        #[arg(short, long, default_value = "127.0.0.1:9400")]
        mgs: String,
        /// Data directory for metadata
        #[arg(short, long, default_value = "/tmp/rustre/mds")]
        data_dir: String,
    },
    /// Start an Object Storage Server (OSS)
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
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Mgs { listen, data_dir } => {
            mgs::run(&listen, &data_dir).await?;
        }
        Commands::Mds {
            listen,
            mgs,
            data_dir,
        } => {
            mds::run(&listen, &mgs, &data_dir).await?;
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
