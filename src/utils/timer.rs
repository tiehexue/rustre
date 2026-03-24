use crate::error::Result;
use std::time::Instant;
use tracing::{debug, error};

/// RAII helper for timing command execution
pub struct CommandTimer {
    command: String,
    start: Instant,
}

impl CommandTimer {
    pub fn new(command: impl Into<String>) -> Self {
        let command = command.into();
        debug!("Starting command: {}", command);
        Self {
            command,
            start: Instant::now(),
        }
    }

    pub fn finish(self, result: &Result<()>) {
        let elapsed = self.start.elapsed();
        match result {
            Ok(()) => {
                debug!(
                    "Command '{}' completed successfully in {:?}",
                    self.command, elapsed
                );
            }
            Err(e) => {
                error!(
                    "Command '{}' failed after {:?}: {}",
                    self.command, elapsed, e
                );
            }
        }
    }
}
