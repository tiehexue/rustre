//! Metadata Server (MDS)
//!
//! The MDS manages the filesystem namespace:
//! - Inode allocation and management
//! - Directory hierarchy (lookup, create, mkdir, readdir, unlink)
//! - Stripe layout computation for new files (round-robin across OSTs)
//! - Registers itself with the MGS on startup
//!
//! All metadata is persisted in FoundationDB via `FdbMdsStore`.
//! This makes MDS completely stateless — multiple MDS instances can run
//! concurrently for high availability. Any MDS can serve any request since
//! all state lives in FDB with transactional consistency.
//!
//! Stripe layouts are simplified — no per-object list, just parameters.
//! Object IDs are deterministic: derived from (ino, stripe_seq).

pub mod operations;
pub mod path_utils;
pub mod server;

pub use server::run;
