//! Management Server (MGS) — stateless, FoundationDB-backed.
//!
//! The MGS is the cluster's configuration authority. It:
//! - Accepts registrations from MDS and OSS nodes
//! - Maintains the authoritative cluster configuration in FoundationDB
//! - Serves configuration to clients so they know where MDS/OSS live
//! - Sends heartbeats to registered MDS/OSS nodes every second
//! - Removes nodes that fail 2 consecutive heartbeats
//!
//! Because all state lives in FDB, multiple MGS instances can run
//! simultaneously for high availability — they are pure stateless proxies.

#[cfg(feature = "fdb")]
pub mod config;
#[cfg(feature = "fdb")]
pub mod heartbeat;
#[cfg(feature = "fdb")]
pub mod server;
#[cfg(feature = "fdb")]
pub use server::run;
