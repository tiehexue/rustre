//! Logging configuration for Rustre

use std::fs::OpenOptions;
use std::io::{self, Write};
use std::sync::Mutex;
use tracing_subscriber::fmt::time::LocalTime;

/// Determine log file name based on subcommand
pub fn get_log_file_name(command: &crate::Commands) -> String {
    match command {
        crate::Commands::Mgs { .. } => "mgs.log".to_string(),
        crate::Commands::Mds { .. } => "mds.log".to_string(),
        crate::Commands::Oss { ost_index, .. } => format!("oss-{}.log", ost_index),
        crate::Commands::Client(_) => "client.log".to_string(),
        crate::Commands::Status { .. } => "status.log".to_string(),
    }
}

/// Custom writer that writes to both stdout and a log file
struct CombinedWriter {
    stdout: io::Stdout,
    file: std::fs::File,
}

impl Write for CombinedWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Write to stdout
        let stdout_result = self.stdout.write(buf);
        // Write to file
        let file_result = self.file.write(buf);
        // Return stdout result (or file result if stdout failed)
        stdout_result.or(file_result)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.stdout.flush()?;
        self.file.flush()
    }
}

/// Initialize logging for the application
pub fn init_logging(command: &crate::Commands, level: &str) -> anyhow::Result<()> {
    // Determine log file name based on subcommand
    let log_file_name = get_log_file_name(command);

    // Create logs directory if it doesn't exist
    std::fs::create_dir_all("logs")?;

    // Open log file for appending
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(format!("logs/{}", log_file_name))?;

    // Create combined writer
    let combined_writer = CombinedWriter {
        stdout: io::stdout(),
        file: log_file,
    };

    // Create a timer with local time and RFC 3339 format (includes milliseconds)
    let timer = LocalTime::rfc_3339();

    // Initialize tracing with the combined writer and local timer
    tracing_subscriber::fmt()
        .with_timer(timer)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level)),
        )
        .with_writer(Mutex::new(combined_writer))
        .init();

    Ok(())
}
