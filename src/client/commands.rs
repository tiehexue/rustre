//! Client command definitions

use clap::Subcommand;

#[derive(Subcommand)]
pub enum ClientCommands {
    /// Write a local file or directory into Rustre (striped across OSTs)
    Put {
        /// MGS address
        #[arg(short, long, default_value = "127.0.0.1:9400")]
        mgs: String,
        /// Local source file or directory path
        source: String,
        /// Destination path in Rustre namespace
        dest: String,
        /// Stripe count (0 = all available OSTs)
        #[arg(short = 'c', long, default_value = "0")]
        stripe_count: u32,
        /// Stripe size in bytes
        #[arg(short = 'S', long, default_value = "1048576")]
        stripe_size: u64,
        /// Replica count
        #[arg(short = 'R', long, default_value = "1")]
        replica_count: u32,
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
