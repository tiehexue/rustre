//! File-backed object store for OSS — enables zero-copy reads and writes.
//!
//! Objects are stored as plain files:
//!   `{data_dir}/objects/{ino_prefix}/{ino}_{chunk_index}`
//!
//! Example: object `0000000000000001:00000005` → `objects/00000000000/0000000000000001_00000005`
//!
//! Because objects are ordinary files, the kernel can sendfile() them directly
//! between disk and socket — no userspace copies needed.

use crate::error::{Result, RustreError};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use tracing::debug;

/// Number of leading hex chars from object_id used as subdirectory prefix.
const PREFIX_LEN: usize = 11;

/// File-backed object store that enables zero-copy operations
pub struct FileObjectStore {
    data_dir: PathBuf,
}

impl FileObjectStore {
    pub fn new(data_dir: &str) -> Result<Self> {
        let objects_dir = PathBuf::from(data_dir).join("objects");
        fs::create_dir_all(&objects_dir).map_err(|e| {
            RustreError::Io(std::io::Error::new(
                e.kind(),
                format!("creating objects directory {}: {e}", objects_dir.display()),
            ))
        })?;

        Ok(Self {
            data_dir: PathBuf::from(data_dir),
        })
    }

    /// Convert object_id to file path
    /// object_id format: "{ino:016x}:{chunk:08x}" e.g. "0000000000000001:00000005"
    fn object_path(&self, object_id: &str) -> PathBuf {
        // Use first 11 chars of ino as subdirectory for better filesystem performance
        let prefix = &object_id[..PREFIX_LEN.min(object_id.len())];
        // Replace : with _ for filename
        let filename = object_id.replace(':', "_");
        self.data_dir.join("objects").join(prefix).join(filename)
    }

    /// Ensure the subdirectory for an object exists
    fn ensure_subdir(&self, object_id: &str) -> Result<()> {
        let path = self.object_path(object_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                RustreError::Io(std::io::Error::new(
                    e.kind(),
                    format!("creating directory {}: {e}", parent.display()),
                ))
            })?;
        }
        Ok(())
    }

    /// Write data to an object file (copy-based, synced to disk).
    pub async fn write(&self, object_id: &str, data: &[u8]) -> Result<()> {
        self.ensure_subdir(object_id)?;
        let path = self.object_path(object_id);
        let data_len = data.len();
        let data = data.to_vec();

        tokio::task::spawn_blocking(move || {
            let mut file = File::create(&path).map_err(|e| {
                RustreError::Io(std::io::Error::new(
                    e.kind(),
                    format!("creating {}: {e}", path.display()),
                ))
            })?;
            file.write_all(&data).map_err(|e| {
                RustreError::Io(std::io::Error::new(
                    e.kind(),
                    format!("writing {}: {e}", path.display()),
                ))
            })?;
            file.sync_all().map_err(|e| {
                RustreError::Io(std::io::Error::new(
                    e.kind(),
                    format!("syncing {}: {e}", path.display()),
                ))
            })?;
            Ok::<(), RustreError>(())
        })
        .await
        .map_err(|e| RustreError::Internal(format!("spawn_blocking: {e}")))??;

        debug!("wrote {data_len} bytes to object {object_id}");
        Ok(())
    }

    /// Open an object file for zero-copy read. Returns `(File, file_length)` for use with sendfile.
    pub fn open_for_zerocopy(&self, object_id: &str) -> Result<(File, u64)> {
        let path = self.object_path(object_id);
        let file = File::open(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                RustreError::NotFound(format!("object {object_id}"))
            } else {
                RustreError::Io(std::io::Error::new(
                    e.kind(),
                    format!("opening {}: {e}", path.display()),
                ))
            }
        })?;
        let len = file
            .metadata()
            .map_err(|e| {
                RustreError::Io(std::io::Error::new(
                    e.kind(),
                    format!("getting metadata for {}: {e}", path.display()),
                ))
            })?
            .len();
        Ok((file, len))
    }

    /// Delete an object
    pub async fn delete(&self, object_id: &str) -> Result<()> {
        let path = self.object_path(object_id);

        tokio::task::spawn_blocking(move || {
            if path.exists() {
                fs::remove_file(&path).map_err(|e| {
                    RustreError::Io(std::io::Error::new(
                        e.kind(),
                        format!("deleting {}: {e}", path.display()),
                    ))
                })?;
            }
            Ok(())
        })
        .await
        .map_err(|e| RustreError::Internal(format!("spawn_blocking: {e}")))?
    }

    /// Delete all objects for a given inode (prefix scan)
    pub async fn delete_inode(&self, ino: u64) -> Result<()> {
        let prefix = format!("{:016x}_", ino);
        let prefix_dir = prefix[..PREFIX_LEN.min(prefix.len())].to_string();
        let search_dir = self.data_dir.join("objects").join(&prefix_dir);

        tokio::task::spawn_blocking(move || {
            if !search_dir.exists() {
                return Ok(());
            }

            for entry in fs::read_dir(&search_dir).map_err(|e| {
                RustreError::Io(std::io::Error::new(
                    e.kind(),
                    format!("reading directory {}: {e}", search_dir.display()),
                ))
            })? {
                let entry = entry.map_err(|e| {
                    RustreError::Io(std::io::Error::new(
                        e.kind(),
                        format!("reading directory entry: {e}"),
                    ))
                })?;
                let filename = entry.file_name();
                let filename_str = filename.to_string_lossy();
                if filename_str.starts_with(&prefix) {
                    fs::remove_file(entry.path()).map_err(|e| {
                        RustreError::Io(std::io::Error::new(
                            e.kind(),
                            format!("deleting {}: {e}", entry.path().display()),
                        ))
                    })?;
                }
            }
            Ok(())
        })
        .await
        .map_err(|e| RustreError::Internal(format!("spawn_blocking: {e}")))?
    }

    /// Total disk usage of all objects
    pub async fn total_usage(&self) -> Result<u64> {
        let objects_dir = self.data_dir.join("objects");

        tokio::task::spawn_blocking(move || {
            let mut total = 0u64;
            if !objects_dir.exists() {
                return Ok(total);
            }

            fn scan_dir(dir: &Path, total: &mut u64) -> std::io::Result<()> {
                for entry in fs::read_dir(dir)? {
                    let entry = entry?;
                    let path = entry.path();
                    if path.is_dir() {
                        scan_dir(&path, total)?;
                    } else {
                        *total += entry.metadata()?.len();
                    }
                }
                Ok(())
            }

            scan_dir(&objects_dir, &mut total).map_err(|e| {
                RustreError::Io(std::io::Error::new(
                    e.kind(),
                    format!("scanning {}: {e}", objects_dir.display()),
                ))
            })?;
            Ok(total)
        })
        .await
        .map_err(|e| RustreError::Internal(format!("spawn_blocking: {e}")))?
    }
}
