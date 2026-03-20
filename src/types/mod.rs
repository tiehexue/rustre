//! File and metadata types

use serde::{Deserialize, Serialize};
use std::time::SystemTime;

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

    /// Find the position of a given primary OST in the stripe layout.
    ///
    /// Returns the index into `ost_indices` (or the computed round-robin position)
    /// for the given primary OST index.
    fn position_of_ost(&self, primary_ost_index: u32) -> Option<usize> {
        if !self.ost_indices.is_empty() {
            self.ost_indices
                .iter()
                .position(|&idx| idx == primary_ost_index)
        } else {
            let pos = (primary_ost_index as i64 - self.stripe_offset as i64)
                .rem_euclid(self.stripe_count as i64) as usize;
            (pos < self.stripe_count as usize).then_some(pos)
        }
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
}

/// Status information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusInfo {
    pub mds_count: usize,
    pub ost_count: usize,
    pub osts: Vec<OstInfo>,
    pub mds_list: Vec<MdsInfo>,
}
