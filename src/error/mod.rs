//! Error types for Rustre

use thiserror::Error;

#[derive(Debug, Error)]
pub enum RustreError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("network error: {0}")]
    Net(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[cfg(feature = "fdb")]
    #[error("already exists: {0}")]
    AlreadyExists(String),
    #[error("is a directory: {0}")]
    IsDirectory(String),
    #[cfg(feature = "fdb")]
    #[error("not a directory: {0}")]
    NotADirectory(String),
    #[cfg(feature = "fdb")]
    #[error("directory not empty: {0}")]
    DirNotEmpty(String),
    #[cfg(feature = "fdb")]
    #[error("no OST available")]
    NoOstAvailable,
    #[error("serialization error: {0}")]
    Serialization(String),
    #[cfg(feature = "fdb")]
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("internal error: {0}")]
    Internal(String),
    #[cfg(feature = "fdb")]
    #[error("FoundationDB error: {0}")]
    Fdb(String),
    #[error("zero-copy error: {0}")]
    ZeroCopyError(String),
}

pub type Result<T> = std::result::Result<T, RustreError>;
