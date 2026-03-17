//! Common types shared across all Rustre components.

use serde::{Deserialize, Serialize};
use std::time::SystemTime;

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
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

// ---------------------------------------------------------------------------
// File / metadata types
// ---------------------------------------------------------------------------

/// Default stripe size: 1 MiB
pub const DEFAULT_STRIPE_SIZE: u64 = 1_048_576;

/// Describes how a file's data is laid out across OSTs (RAID-0 striping).
///
/// Object IDs and OST assignments are deterministic:
///   object_id(ino, seq) = format!("{:016x}:{:08x}", ino, seq)
///   ost_for_chunk(seq)  = (stripe_offset + seq) % total_ost_count
///
/// When stripe_count < total_ost_count, the specific OST indices used
/// are stored in ost_indices to ensure correct mapping.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StripeLayout {
    /// Number of OSTs the file is striped across
    pub stripe_count: u32,
    /// Size of each stripe chunk in bytes (default: 1 MiB)
    pub stripe_size: u64,
    /// Starting OST index (typically ino % ost_count)
    pub stripe_offset: u32,
    /// Specific OST indices used for this file (when stripe_count < total_ost_count)
    /// If empty, uses round-robin across all OSTs starting from stripe_offset
    #[serde(default)]
    pub ost_indices: Vec<u32>,
}

impl StripeLayout {
    /// Compute the deterministic object ID for a given inode and stripe sequence.
    pub fn object_id(ino: u64, stripe_seq: u32) -> String {
        format!("{:016x}:{:08x}", ino, stripe_seq)
    }

    /// Compute which OST a given stripe sequence lands on.
    #[allow(dead_code)]
    pub fn ost_for_chunk(&self, stripe_seq: u32) -> u32 {
        if !self.ost_indices.is_empty() {
            // Use specific OST indices when provided
            let idx = (stripe_seq as usize) % self.ost_indices.len();
            self.ost_indices[idx]
        } else {
            // Fall back to round-robin across all OSTs
            (self.stripe_offset + stripe_seq) % self.stripe_count
        }
    }

    /// Total number of stripe chunks for a file of the given size.
    pub fn total_chunks(&self, file_size: u64) -> u32 {
        if file_size == 0 {
            return 0;
        }
        file_size.div_ceil(self.stripe_size) as u32
    }
}

/// Inode-level metadata for a file or directory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMeta {
    /// Unique inode number
    pub ino: u64,
    /// File name (basename)
    pub name: String,
    /// Full path
    pub path: String,
    /// Is this a directory?
    pub is_dir: bool,
    /// Total file size in bytes
    pub size: u64,
    /// Creation timestamp (seconds since epoch)
    pub ctime: u64,
    /// Modification timestamp
    pub mtime: u64,
    /// Stripe layout (None for directories)
    pub layout: Option<StripeLayout>,
    /// Parent inode number
    pub parent_ino: u64,
}

impl FileMeta {
    pub fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }
}

// ---------------------------------------------------------------------------
// OST info (registered with MGS)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OstInfo {
    pub ost_index: u32,
    pub address: String,
    pub total_bytes: u64,
    pub used_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MdsInfo {
    pub address: String,
}

// ---------------------------------------------------------------------------
// Cluster configuration (held by MGS)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClusterConfig {
    pub mds_list: Vec<MdsInfo>,
    pub ost_list: Vec<OstInfo>,
}

// ---------------------------------------------------------------------------
// RPC messages — the wire protocol between all components
// ---------------------------------------------------------------------------

/// Every message on the wire is a `RpcMessage` serialised with bincode,
/// length-prefixed with a 4-byte big-endian u32.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcMessage {
    pub id: u64,
    pub kind: RpcKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RpcKind {
    // -- MGS RPCs --
    RegisterMds(MdsInfo),
    RegisterOst(OstInfo),
    GetConfig,
    GetConfigReply(ClusterConfig),
    UpdateOstUsage {
        ost_index: u32,
        used_bytes: u64,
    },

    // -- MDS RPCs --
    Lookup(String),    // path → FileMeta
    Create(CreateReq), // create file, returns FileMeta with layout
    Mkdir(String),     // create directory
    Readdir(String),   // list directory → Vec<FileMeta>
    Unlink(String),    // remove file
    Stat(String),      // stat → FileMeta
    SetSize {
        path: String,
        size: u64,
    },

    // -- OSS RPCs --
    ObjWrite(ObjWriteReq),
    ObjRead(ObjReadReq),
    ObjDelete {
        object_id: String,
    },
    /// Delete all objects for an inode (bulk cleanup)
    ObjDeleteInode {
        ino: u64,
    },

    // -- Generic replies --
    Ok,
    Error(String),
    MetaReply(FileMeta),
    MetaListReply(Vec<FileMeta>),
    DataReply(Vec<u8>),
    ConfigReply(ClusterConfig),
    StatusReply(StatusInfo),

    // -- Status --
    GetStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateReq {
    pub path: String,
    pub stripe_count: u32,
    pub stripe_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjWriteReq {
    pub object_id: String,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjReadReq {
    pub object_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusInfo {
    pub mds_count: usize,
    pub ost_count: usize,
    pub osts: Vec<OstInfo>,
    pub mds_list: Vec<MdsInfo>,
}
