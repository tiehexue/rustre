//! Storage engines for Rustre.
//!
//! - `FileObjectStore`: File-backed object storage for OSS (enables zero-copy reads/writes).
//! - `RocksObjectStore`: RocksDB-backed object storage for OSS (legacy).
//! - `FdbMetaStore`: FoundationDB-backed metadata store for MGS (distributed, HA).
//! - `FdbMdsStore`: FoundationDB-backed metadata store for MDS (distributed, stateless HA).

pub mod fdb_mds_store;
pub mod fdb_meta_store;
pub mod file_store;

pub use fdb_mds_store::FdbMdsStore;
pub use fdb_meta_store::FdbMetaStore;
pub use file_store::FileObjectStore;
