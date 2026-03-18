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
    #[error("already exists: {0}")]
    AlreadyExists(String),
    #[error("is a directory: {0}")]
    IsDirectory(String),
    #[error("not a directory: {0}")]
    NotADirectory(String),
    #[error("directory not empty: {0}")]
    DirNotEmpty(String),
    #[error("no OST available")]
    NoOstAvailable,
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("internal error: {0}")]
    Internal(String),
    #[error("FoundationDB error: {0}")]
    Fdb(String),
}

pub type Result<T> = std::result::Result<T, RustreError>;
