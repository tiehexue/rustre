//! Storage engines for Rustre.
//!
//! - [`FileObjectStore`]: File-backed object storage for OSS (zero-copy reads/writes).
//! - [`FdbMetaStore`]: FoundationDB-backed cluster state store for MGS.
//! - [`FdbMdsStore`]: FoundationDB-backed metadata store for MDS.

#[cfg(feature = "fdb")]
pub mod fdb_mds_store;
#[cfg(feature = "fdb")]
pub mod fdb_meta_store;
pub mod file_store;

#[cfg(feature = "fdb")]
pub use fdb_mds_store::FdbMdsStore;
#[cfg(feature = "fdb")]
pub use fdb_meta_store::FdbMetaStore;
pub use file_store::FileObjectStore;
