//! Storage engines for Rustre.
//!
//! - `RocksObjectStore`: RocksDB-backed object storage for OSS (replaces flat files).
//! - `FdbMetaStore`: FoundationDB-backed metadata store for MGS (distributed, HA).
//! - `FdbMdsStore`: FoundationDB-backed metadata store for MDS (distributed, stateless HA).

pub mod fdb_mds_store;
pub mod fdb_meta_store;
pub mod rocksdb_store;

pub use fdb_mds_store::FdbMdsStore;
pub use fdb_meta_store::FdbMetaStore;
pub use rocksdb_store::RocksObjectStore;
