//! Local storage engine — flat-file object store + metadata persistence.
//!
//! Used by OSS to store data objects and by MDS to persist metadata.

use crate::common::{Result, RustreError};
use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::Mutex;
use tracing::debug;

/// Simple on-disk object store. Each object is a file under `base_dir/objects/`.
/// Uses per-object locks to prevent concurrent read-modify-write races.
pub struct ObjectStore {
    objects_dir: PathBuf,
    locks: DashMap<String, Arc<Mutex<()>>>,
}

impl ObjectStore {
    pub async fn new(base_dir: &str) -> Result<Self> {
        let objects_dir = PathBuf::from(base_dir).join("objects");
        fs::create_dir_all(&objects_dir).await?;
        Ok(Self {
            objects_dir,
            locks: DashMap::new(),
        })
    }

    fn object_path(&self, object_id: &str) -> PathBuf {
        self.objects_dir.join(object_id)
    }

    fn get_lock(&self, object_id: &str) -> Arc<Mutex<()>> {
        self.locks
            .entry(object_id.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Write data at the given offset within an object (with per-object locking).
    pub async fn write(&self, object_id: &str, offset: u64, data: &[u8]) -> Result<()> {
        let lock = self.get_lock(object_id);
        let _guard = lock.lock().await;

        let path = self.object_path(object_id);
        // Read existing data (or empty), expand if needed, write back
        let mut buf = if path.exists() {
            fs::read(&path).await?
        } else {
            Vec::new()
        };
        let end = offset as usize + data.len();
        if buf.len() < end {
            buf.resize(end, 0);
        }
        buf[offset as usize..end].copy_from_slice(data);
        fs::write(&path, &buf).await?;
        debug!(
            "wrote {} bytes to object {object_id} @ offset {offset}",
            data.len()
        );
        Ok(())
    }

    /// Read `length` bytes starting at `offset` from an object.
    pub async fn read(&self, object_id: &str, offset: u64, length: u64) -> Result<Vec<u8>> {
        let path = self.object_path(object_id);
        if !path.exists() {
            return Err(RustreError::NotFound(format!("object {object_id}")));
        }
        let buf = fs::read(&path).await?;
        let start = offset as usize;
        let end = std::cmp::min(start + length as usize, buf.len());
        if start >= buf.len() {
            return Ok(Vec::new());
        }
        Ok(buf[start..end].to_vec())
    }

    /// Delete an object.
    pub async fn delete(&self, object_id: &str) -> Result<()> {
        let path = self.object_path(object_id);
        if path.exists() {
            fs::remove_file(&path).await?;
        }
        Ok(())
    }

    /// Get the total size of an object on disk.
    #[allow(dead_code)]
    pub async fn object_size(&self, object_id: &str) -> Result<u64> {
        let path = self.object_path(object_id);
        if !path.exists() {
            return Ok(0);
        }
        let meta = fs::metadata(&path).await?;
        Ok(meta.len())
    }

    /// Total disk usage of all objects.
    pub async fn total_usage(&self) -> Result<u64> {
        let mut total = 0u64;
        let mut entries = fs::read_dir(&self.objects_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let meta = entry.metadata().await?;
            total += meta.len();
        }
        Ok(total)
    }
}

/// Simple JSON-file-based metadata store.
pub struct MetaStore {
    base_dir: PathBuf,
}

impl MetaStore {
    pub async fn new(base_dir: &str) -> Result<Self> {
        let base = PathBuf::from(base_dir);
        fs::create_dir_all(&base).await?;
        Ok(Self { base_dir: base })
    }

    fn meta_path(&self, name: &str) -> PathBuf {
        // Encode path as safe filename
        let safe = name.replace('/', "__SLASH__");
        self.base_dir.join(format!("{safe}.json"))
    }

    pub async fn save<T: serde::Serialize>(&self, key: &str, value: &T) -> Result<()> {
        let path = self.meta_path(key);
        let data = serde_json::to_vec_pretty(value)
            .map_err(|e| RustreError::Serialization(e.to_string()))?;
        fs::write(&path, data).await?;
        Ok(())
    }

    pub async fn load<T: serde::de::DeserializeOwned>(&self, key: &str) -> Result<T> {
        let path = self.meta_path(key);
        if !path.exists() {
            return Err(RustreError::NotFound(format!("meta key: {key}")));
        }
        let data = fs::read(&path).await?;
        serde_json::from_slice(&data).map_err(|e| RustreError::Serialization(e.to_string()))
    }

    pub async fn exists(&self, key: &str) -> bool {
        self.meta_path(key).exists()
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        let path = self.meta_path(key);
        if path.exists() {
            fs::remove_file(&path).await?;
        }
        Ok(())
    }

    /// List all keys that start with a prefix.
    #[allow(dead_code)]
    pub async fn list_keys(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut entries = fs::read_dir(&self.base_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            if let Some(name) = entry.file_name().to_str() {
                if name.ends_with(".json") {
                    let key = name.trim_end_matches(".json").replace("__SLASH__", "/");
                    if key.starts_with(prefix) || prefix.is_empty() {
                        keys.push(key);
                    }
                }
            }
        }
        Ok(keys)
    }
}
