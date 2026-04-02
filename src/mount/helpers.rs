//! Error mapping and conversion helpers for FUSE.

use crate::error::RustreError;
use crate::types::FileMeta;
use fuser::{Errno, FileAttr, FileType, INodeNo};
use std::time::{Duration, UNIX_EPOCH};

use super::types::BLOCK_SIZE;

// ---------------------------------------------------------------------------
// Error mapping: RustreError → Errno
// ---------------------------------------------------------------------------

pub fn error_to_errno(e: &RustreError) -> Errno {
    match e {
        RustreError::NotFound(_) => Errno::ENOENT,
        #[cfg(feature = "fdb")]
        RustreError::AlreadyExists(_) => Errno::EEXIST,
        RustreError::IsDirectory(_) => Errno::EISDIR,
        #[cfg(feature = "fdb")]
        RustreError::NotADirectory(_) => Errno::ENOTDIR,
        #[cfg(feature = "fdb")]
        RustreError::DirNotEmpty(_) => Errno::ENOTEMPTY,
        #[cfg(feature = "fdb")]
        RustreError::NoOstAvailable => Errno::ENOSPC,
        #[cfg(feature = "fdb")]
        RustreError::InvalidArgument(_) => Errno::EINVAL,
        RustreError::Io(ref io_err) => Errno::from(io_err.kind()),
        RustreError::Net(_) => Errno::EIO,
        _ => Errno::EIO,
    }
}

pub fn rustre_err_to_errno(e: RustreError) -> Errno {
    error_to_errno(&e)
}

/// Map an MDS error string to the most appropriate errno.
pub fn mds_error_to_errno(e: &str) -> Errno {
    if e.contains("not found") {
        Errno::ENOENT
    } else if e.contains("already exists") {
        Errno::EEXIST
    } else if e.contains("not a directory") {
        Errno::ENOTDIR
    } else if e.contains("directory not empty") || e.contains("not empty") {
        Errno::ENOTEMPTY
    } else if e.contains("invalid argument") {
        Errno::EINVAL
    } else if e.contains("is a directory") {
        Errno::EISDIR
    } else if e.contains("cannot hard-link") {
        Errno::EPERM
    } else {
        Errno::EIO
    }
}

// ---------------------------------------------------------------------------
// FileMeta → FileAttr conversion
// ---------------------------------------------------------------------------

pub fn meta_to_attr(meta: &FileMeta) -> FileAttr {
    let kind = if meta.is_dir {
        FileType::Directory
    } else if meta.symlink_target.is_some() {
        FileType::Symlink
    } else {
        FileType::RegularFile
    };
    let ctime = UNIX_EPOCH + Duration::from_secs(meta.ctime);
    let mtime = UNIX_EPOCH + Duration::from_secs(meta.mtime);

    // Calculate block count: number of 512-byte blocks
    let blocks = meta.size.div_ceil(512);

    FileAttr {
        ino: INodeNo(meta.ino),
        size: meta.size,
        blocks,
        atime: mtime, // no separate atime in Rustre, use mtime
        mtime,
        ctime,
        crtime: ctime,
        kind,
        perm: (meta.mode & 0o777) as u16,
        nlink: meta.nlink,
        uid: meta.uid,
        gid: meta.gid,
        rdev: 0,
        blksize: BLOCK_SIZE,
        flags: 0,
    }
}
