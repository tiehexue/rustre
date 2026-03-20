//! Rustre Client
//!
//! Analogous to Lustre's client module stack (LLite + LOV + OSC + MDC).
//! 1. Fetches cluster config from MGS (discovers MDS + OST addresses)
//! 2. Talks to MDS for metadata (create, lookup, readdir, unlink, stat)
//! 3. Talks directly to OSTs for data — RAID-0 striped writes and parallel reads
//!
//! Object IDs and OST assignments are deterministic: derived from (ino, stripe_seq)
//! using [`StripeLayout`] helpers — no per-object metadata required.

pub mod commands;
pub mod operations;
pub mod put;
pub mod rm;
pub mod status;

pub use operations::run;
pub use status::status;
