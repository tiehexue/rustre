//! Rustre Client
//!
//! The client is analogous to Lustre's client module stack (LLite + LOV + OSC + MDC).
//! It:
//! 1. Fetches cluster config from MGS (learning MDS + OST addresses)
//! 2. Talks to MDS for metadata ops (create, lookup, readdir, unlink, stat)
//! 3. Talks directly to OSTs for data I/O — RAID-0 striped writes and parallel reads
//!
//! Object IDs and OST assignments are now computed deterministically from
//! (ino, stripe_seq) using StripeLayout helper methods — no need to store
//! per-object lists in metadata.

pub mod commands;
pub mod operations;
pub mod put;
pub mod status;

pub use operations::run;
pub use status::status;
