//! File and metadata types

use serde::{Deserialize, Serialize};

/// Default stripe size: 1 MiB
#[cfg(feature = "fdb")]
pub const DEFAULT_STRIPE_SIZE: u64 = 1_048_576;

/// Describes how a file's data is laid out across OSTs (RAID-0 striping).
///
/// Object IDs and OST assignments are deterministic:
///   object_id(ino, seq) = format!("{:016x}:{:08x}", ino, seq)
///   ost_for_chunk(seq)  = ost_indices[seq % ost_indices.len()]
///
/// The OST indices used for striping are stored in ost_indices.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StripeLayout {
    /// Size of each stripe chunk in bytes (default: 1 MiB)
    pub stripe_size: u64,
    /// Specific OST indices used for this file
    #[serde(default)]
    pub ost_indices: Vec<u32>,
    /// Number of replicas for each stripe (default: 1, no replication)
    pub replica_count: u32,
    /// Replica mapping: for each OST index in ost_indices, contains replica_count replica OST indices
    /// If empty, replicas are not configured or replica_count is 1
    #[serde(default)]
    pub replica_map: Vec<Vec<u32>>,
}

impl StripeLayout {
    /// Compute the deterministic object ID for a given inode and stripe sequence.
    pub fn object_id(ino: u64, stripe_seq: u32) -> String {
        format!("{:016x}:{:08x}", ino, stripe_seq)
    }

    /// Compute which OST a given stripe sequence lands on.
    pub fn ost_for_chunk(&self, stripe_seq: u32) -> u32 {
        // ost_indices should never be empty, but guard against it
        if self.ost_indices.is_empty() {
            return 0;
        }
        let idx = (stripe_seq as usize) % self.ost_indices.len();
        self.ost_indices[idx]
    }

    /// Total number of stripe chunks for a file of the given size.
    pub fn total_chunks(&self, file_size: u64) -> u32 {
        if file_size == 0 {
            return 0;
        }
        file_size.div_ceil(self.stripe_size) as u32
    }

    /// Find the position of a given primary OST in the stripe layout.
    ///
    /// Returns the index into `ost_indices` for the given primary OST index.
    fn position_of_ost(&self, primary_ost_index: u32) -> Option<usize> {
        self.ost_indices
            .iter()
            .position(|&idx| idx == primary_ost_index)
    }

    /// Collect replica OST indices for a given primary OST index.
    ///
    /// Returns an empty vec when replication is disabled (replica_count ≤ 1)
    /// or when the primary OST has no configured replicas.
    pub fn replica_ost_indices(&self, primary_ost_index: u32) -> Vec<u32> {
        if self.replica_count <= 1 || self.replica_map.is_empty() {
            return Vec::new();
        }
        self.position_of_ost(primary_ost_index)
            .and_then(|pos| self.replica_map.get(pos))
            .cloned()
            .unwrap_or_default()
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
    /// File mode (permissions + file type bits)
    #[serde(default = "default_mode")]
    pub mode: u32,
    /// User ID of owner
    #[serde(default = "default_uid")]
    pub uid: u32,
    /// Group ID of owner
    #[serde(default = "default_gid")]
    pub gid: u32,
    /// Stripe layout (None for directories)
    pub layout: Option<StripeLayout>,
    /// Parent inode number
    pub parent_ino: u64,
    /// Write-intent flag: true while data is still being written to OSS.
    /// Pending files are invisible to Stat/Readdir/Lookup and will be
    /// garbage-collected if never committed (client crash recovery).
    #[serde(default)]
    pub pending: bool,
    /// Hard link count. Defaults to 1 for backward compatibility.
    #[serde(default = "default_nlink")]
    pub nlink: u32,
    /// Symlink target path (None for regular files/directories)
    #[serde(default)]
    pub symlink_target: Option<String>,
}

fn default_nlink() -> u32 {
    1
}

fn default_mode() -> u32 {
    0o755
}

fn default_uid() -> u32 {
    crate::utils::fid::get_ugid().0
}

fn default_gid() -> u32 {
    crate::utils::fid::get_ugid().1
}

impl FileMeta {
    #[cfg(any(feature = "fdb", feature = "fuse"))]
    pub fn now_secs() -> u64 {
        use std::time::SystemTime;
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }
}

/// OST info (registered with MGS)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OstInfo {
    pub ost_index: u32,
    pub address: String,
    pub total_bytes: u64,
    pub used_bytes: u64,
}

/// MDS info (registered with MGS)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MdsInfo {
    pub address: String,
}

/// Cluster configuration (held by MGS)
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClusterConfig {
    pub mds_list: Vec<MdsInfo>,
    pub ost_list: Vec<OstInfo>,
}

/// Create request for files
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateReq {
    pub path: String,
    pub stripe_count: u32,
    pub stripe_size: u64,
    pub replica_count: u32,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
}

/// Status information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusInfo {
    pub mds_count: usize,
    pub ost_count: usize,
    pub osts: Vec<OstInfo>,
    pub mds_list: Vec<MdsInfo>,
}
