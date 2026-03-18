//! RocksDB-backed object store for OSS

use crate::error::{Result, RustreError};
use rocksdb::{IteratorMode, Options, DB};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::debug;

/// RocksDB-backed object store. Each object is a key-value pair:
///   Key:   `obj:{ino_hex}:{stripe_seq_hex}`
///   Value: raw bytes of the stripe chunk
pub struct RocksObjectStore {
    db: Arc<DB>,
}

impl RocksObjectStore {
    pub fn new(data_dir: &str) -> Result<Self> {
        let path = PathBuf::from(data_dir).join("rocksdb");
        let mut opts = Options::default();
        opts.create_if_missing(true);
        // Optimise for bulk writes: larger write buffer, more parallelism
        opts.set_write_buffer_size(64 * 1024 * 1024); // 64 MB
        opts.set_max_write_buffer_number(4);
        opts.set_target_file_size_base(64 * 1024 * 1024);
        opts.increase_parallelism(num_cpus());

        let db = DB::open(&opts, &path).map_err(|e| {
            RustreError::Internal(format!("RocksDB open failed at {}: {e}", path.display()))
        })?;
        Ok(Self { db: Arc::new(db) })
    }

    fn obj_key(object_id: &str) -> Vec<u8> {
        format!("obj:{object_id}").into_bytes()
    }

    /// Write data at the given offset within an object.
    /// For simplicity, does a read-modify-write under RocksDB's internal locking.
    pub async fn write(&self, object_id: &str, data: &[u8]) -> Result<()> {
        let db = Arc::clone(&self.db);
        let key = Self::obj_key(object_id);
        let data_len = data.len();
        let data = data.to_vec();
        let oid = object_id.to_string();

        tokio::task::spawn_blocking(move || {
            db.put(&key, data)
                .map_err(|e| RustreError::Internal(format!("RocksDB put: {e}")))
        })
        .await
        .map_err(|e| RustreError::Internal(format!("spawn_blocking: {e}")))??;

        debug!("wrote {data_len} bytes to object {oid}");
        Ok(())
    }

    /// Read `length` bytes starting at `offset` from an object.
    pub async fn read(&self, object_id: &str) -> Result<Vec<u8>> {
        let db = Arc::clone(&self.db);
        let key = Self::obj_key(object_id);
        let oid = object_id.to_string();

        tokio::task::spawn_blocking(move || match db.get(&key) {
            Ok(Some(buf)) => Ok(buf),
            Ok(None) => Err(RustreError::NotFound(format!("object {oid}"))),
            Err(e) => Err(RustreError::Internal(format!("RocksDB get: {e}"))),
        })
        .await
        .map_err(|e| RustreError::Internal(format!("spawn_blocking: {e}")))?
    }

    /// Delete an object.
    pub async fn delete(&self, object_id: &str) -> Result<()> {
        let db = Arc::clone(&self.db);
        let key = Self::obj_key(object_id);

        tokio::task::spawn_blocking(move || {
            db.delete(&key)
                .map_err(|e| RustreError::Internal(format!("RocksDB delete: {e}")))
        })
        .await
        .map_err(|e| RustreError::Internal(format!("spawn_blocking: {e}")))?
    }

    /// Delete all objects for a given inode (prefix scan + delete).
    pub async fn delete_inode(&self, ino: u64) -> Result<()> {
        let db = Arc::clone(&self.db);
        let prefix = format!("obj:{:016x}:", ino).into_bytes();

        tokio::task::spawn_blocking(move || {
            let iter = db.iterator(IteratorMode::From(&prefix, rocksdb::Direction::Forward));
            for item in iter {
                let (key, _) =
                    item.map_err(|e| RustreError::Internal(format!("RocksDB iter: {e}")))?;
                if !key.starts_with(&prefix) {
                    break;
                }
                db.delete(&key)
                    .map_err(|e| RustreError::Internal(format!("RocksDB delete: {e}")))?;
            }
            Ok(())
        })
        .await
        .map_err(|e| RustreError::Internal(format!("spawn_blocking: {e}")))?
    }

    /// Total disk usage of all objects (sum of value sizes).
    pub async fn total_usage(&self) -> Result<u64> {
        let db = Arc::clone(&self.db);
        let prefix = b"obj:".to_vec();

        tokio::task::spawn_blocking(move || {
            let mut total = 0u64;
            let iter = db.iterator(IteratorMode::From(&prefix, rocksdb::Direction::Forward));
            for item in iter {
                let (key, value) =
                    item.map_err(|e| RustreError::Internal(format!("RocksDB iter: {e}")))?;
                if !key.starts_with(&prefix) {
                    break;
                }
                total += value.len() as u64;
            }
            Ok(total)
        })
        .await
        .map_err(|e| RustreError::Internal(format!("spawn_blocking: {e}")))?
    }
}

fn num_cpus() -> i32 {
    std::thread::available_parallelism()
        .map(|n| n.get() as i32)
        .unwrap_or(4)
}
