//! Storage engines for Rustre.
//!
//! - [`FileObjectStore`]: File-backed object storage for OSS (zero-copy reads/writes).
//! - [`FdbMetaStore`]: FoundationDB-backed cluster state store for MGS.
//! - [`FdbMdsStore`]: FoundationDB-backed metadata store for MDS.

pub mod fdb_mds_store;
pub mod fdb_meta_store;
pub mod file_store;

pub use fdb_mds_store::FdbMdsStore;
pub use fdb_meta_store::FdbMetaStore;
pub use file_store::FileObjectStore;
