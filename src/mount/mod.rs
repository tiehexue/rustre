//! FUSE mount — makes Rustre mountable as a POSIX filesystem on macOS/Linux.
//!
//! # Architecture
//!
//! The mount module is split into four submodules for clarity:
//!
//! - [`types`]:    Core data structures (`InodeMap`, `OpenFile`, `InodeState`, `RustreFs`).
//! - [`helpers`]:  Error mapping (`RustreError` → `Errno`) and `FileMeta` → `FileAttr`.
//! - [`ost_io`]:   OST read/write primitives and high-level FUSE I/O methods.
//! - [`fs_impl`]:  The `fuser::Filesystem` trait implementation.
//!
//! # Key design decisions
//!
//! 1. **Immediate commit on create()** — files are committed to MDS at size 0
//!    right after creation, making them immediately visible to lookups and
//!    readdir.  This is critical for `git init`, `cargo build`, and similar
//!    tools that create a file then immediately stat the parent directory.
//!
//! 2. **Open-unlink (detached) semantics** — when a file is unlinked while it
//!    still has open file handles, the MDS record is removed immediately (the
//!    file disappears from readdir/lookup) but OSS object cleanup is deferred
//!    to the last `release()`.  This prevents "file not found" errors in build
//!    tools that keep files open while the build tree is being cleaned.
//!
//! 3. **Per-inode state tracking** — a shared `InodeState` tracks the
//!    reference count, tracked file size, and dirty flag across ALL open
//!    handles for a given inode.  `flush`/`fsync`/`release` flush all handles
//!    for the same inode, not just the one being closed, ensuring cross-fd
//!    read-write consistency (required by git and cargo).
//!
//! 4. **Opendir snapshot** — directory entries are captured at `opendir()` time
//!    so concurrent `unlink()`s during a `readdir()` sequence don't shift
//!    offsets and cause `rm -rf` to miss entries.
//!
//! 5. **Write-through for aligned writes** — whole-chunk writes go directly to
//!    OST via `write_through()`; partial-chunk writes are buffered per-fh and
//!    merged on `flush()`.
//!
//! 6. **mknod fallback** — implements `mknod()` for platforms that don't route
//!    `O_CREAT` through the FUSE `create()` callback.
//!
//! 7. **SetSizeByIno with fallback** — the FUSE client prefers the inode-based
//!    `SetSizeByIno` RPC (avoids path resolution) but falls back to the legacy
//!    path-based `SetSize` for compatibility with older MDS versions.

mod fs_impl;
pub mod helpers;
pub mod ost_io;
pub mod types;

use crate::error::RustreError;
use fuser::{Config, MountOption, SessionACL};
use tracing::info;
use types::RustreFs;

/// Mount the Rustre filesystem at the given mountpoint.
pub fn mount(mgs_addr: &str, mountpoint: &str) -> Result<(), RustreError> {
    info!("Mounting Rustre filesystem at {mountpoint} (MGS: {mgs_addr})");

    let fs = RustreFs::new(mgs_addr)?;

    let mut config = Config::default();
    config.mount_options = vec![
        MountOption::FSName("rustre".to_string()),
        MountOption::AutoUnmount,
        MountOption::DefaultPermissions,
    ];
    config.acl = SessionACL::All;

    fuser::mount2(fs, mountpoint, &config).map_err(|e| {
        RustreError::Io(std::io::Error::new(
            e.kind(),
            format!("FUSE mount failed at {mountpoint}: {e}"),
        ))
    })?;

    Ok(())
}
