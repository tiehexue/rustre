//! Storage engines for Rustre.
//!
//! - `RocksObjectStore`: RocksDB-backed object storage for OSS (replaces flat files).
//! - `FdbMetaStore`: FoundationDB-backed metadata store for MGS (distributed, HA).
//! - `FdbMdsStore`: FoundationDB-backed metadata store for MDS (distributed, stateless HA).

use crate::common::{Result, RustreError};
use rocksdb::{IteratorMode, Options, DB};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::debug;

// ===========================================================================
// RocksDB Object Store (for OSS)
// ===========================================================================

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

// ===========================================================================
// FoundationDB Meta Store (for MGS — distributed, stateless, HA)
// ===========================================================================

/// FoundationDB-backed key-value store for MGS cluster state.
///
/// Key schema under prefix `{cluster}/`:
///   {cluster}/mds/{address}           → MdsInfo (bincode)
///   {cluster}/ost/{ost_index:08x}     → OstInfo (bincode)
///   {cluster}/ost_usage/{index:08x}   → u64 used_bytes (bincode)
pub struct FdbMetaStore {
    db: foundationdb::Database,
    prefix: String,
}

impl FdbMetaStore {
    /// Create a new FDB-backed meta store.
    /// `cluster_name` is used as a key prefix to support multiple clusters on one FDB.
    pub fn new(cluster_name: &str) -> Result<Self> {
        let db = foundationdb::Database::default()
            .map_err(|e| RustreError::Fdb(format!("failed to open FDB: {e}")))?;
        Ok(Self {
            db,
            prefix: format!("{cluster_name}/"),
        })
    }

    fn key(&self, suffix: &str) -> Vec<u8> {
        format!("{}{}", self.prefix, suffix).into_bytes()
    }

    fn range_prefix(&self, prefix_suffix: &str) -> (Vec<u8>, Vec<u8>) {
        let begin = format!("{}{}", self.prefix, prefix_suffix).into_bytes();
        let mut end = begin.clone();
        // Increment last byte to form exclusive upper bound
        if let Some(last) = end.last_mut() {
            *last += 1;
        }
        (begin, end)
    }

    /// Set a key to a bincode-serialized value.
    pub async fn set<T: serde::Serialize>(&self, suffix: &str, value: &T) -> Result<()> {
        let key = self.key(suffix);
        let data =
            bincode::serialize(value).map_err(|e| RustreError::Serialization(e.to_string()))?;

        self.db
            .run(|trx, _maybe_committed| {
                let key = key.clone();
                let data = data.clone();
                async move {
                    trx.set(&key, &data);
                    Ok(())
                }
            })
            .await
            .map_err(|e| RustreError::Fdb(format!("FDB set: {e}")))?;
        Ok(())
    }

    /// Get a bincode-deserialized value by key. Returns None if not found.
    pub async fn get<T: serde::de::DeserializeOwned>(&self, suffix: &str) -> Result<Option<T>> {
        let key = self.key(suffix);

        let result = self
            .db
            .run(|trx, _maybe_committed| {
                let key = key.clone();
                async move { Ok(trx.get(&key, false).await?) }
            })
            .await
            .map_err(|e| RustreError::Fdb(format!("FDB get: {e}")))?;

        match result {
            Some(slice) => {
                let value: T = bincode::deserialize(slice.as_ref())
                    .map_err(|e| RustreError::Serialization(e.to_string()))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Delete a key.
    #[allow(dead_code)]
    pub async fn delete(&self, suffix: &str) -> Result<()> {
        let key = self.key(suffix);

        self.db
            .run(|trx, _maybe_committed| {
                let key = key.clone();
                async move {
                    trx.clear(&key);
                    Ok(())
                }
            })
            .await
            .map_err(|e| RustreError::Fdb(format!("FDB clear: {e}")))?;
        Ok(())
    }

    /// List all values under a key prefix, deserialized as T.
    pub async fn list_prefix<T: serde::de::DeserializeOwned>(
        &self,
        prefix_suffix: &str,
    ) -> Result<Vec<T>> {
        let (begin, end) = self.range_prefix(prefix_suffix);

        let result = self
            .db
            .run(|trx, _maybe_committed| {
                let begin = begin.clone();
                let end = end.clone();
                async move {
                    let range = trx
                        .get_range(
                            &foundationdb::RangeOption::from((begin.as_slice(), end.as_slice())),
                            1, // iteration
                            false,
                        )
                        .await?;
                    Ok(range)
                }
            })
            .await
            .map_err(|e| RustreError::Fdb(format!("FDB get_range: {e}")))?;

        let mut items = Vec::new();
        for kv in result.as_ref() {
            let value: T = bincode::deserialize(kv.value())
                .map_err(|e| RustreError::Serialization(e.to_string()))?;
            items.push(value);
        }
        Ok(items)
    }
}

// ===========================================================================
// FoundationDB MDS Store (stateless, distributed HA for MDS)
// ===========================================================================

/// FoundationDB-backed metadata store for MDS.
///
/// All filesystem metadata lives in FDB, making MDS completely stateless.
/// Multiple MDS instances can run concurrently against the same FDB cluster.
///
/// Key schema under prefix `{cluster}/mds_meta/`:
///
///   `ino/{ino:016x}`                          → FileMeta (bincode)
///   `path/{normalized_path}`                   → u64 ino (bincode, 8 bytes LE)
///   `children/{parent_ino:016x}/{child_ino:016x}` → empty (existence = membership)
///   `next_ino`                                 → u64 (8 bytes LE, atomic add)
///
/// Design rationale:
/// - **Children as individual keys**: avoids read-modify-write on hot directories;
///   adding/removing a child is a single set/clear, listing is a range scan.
/// - **next_ino via FDB atomic add**: safe concurrent inode allocation across
///   multiple MDS instances.
/// - **Transactional creates/unlinks**: inode + path + children updated atomically.
pub struct FdbMdsStore {
    db: foundationdb::Database,
    prefix: String,
}

impl FdbMdsStore {
    /// Create a new FDB-backed MDS store.
    /// `cluster_name` is used as key prefix to support multiple clusters on one FDB.
    pub fn new(cluster_name: &str) -> Result<Self> {
        let db = foundationdb::Database::default()
            .map_err(|e| RustreError::Fdb(format!("failed to open FDB for MDS: {e}")))?;
        Ok(Self {
            db,
            prefix: format!("{cluster_name}/mds_meta/"),
        })
    }

    // -----------------------------------------------------------------------
    // Key construction helpers
    // -----------------------------------------------------------------------

    fn ino_key(&self, ino: u64) -> Vec<u8> {
        format!("{}ino/{:016x}", self.prefix, ino).into_bytes()
    }

    fn path_key(&self, path: &str) -> Vec<u8> {
        format!("{}path/{}", self.prefix, path).into_bytes()
    }

    fn child_key(&self, parent_ino: u64, child_ino: u64) -> Vec<u8> {
        format!(
            "{}children/{:016x}/{:016x}",
            self.prefix, parent_ino, child_ino
        )
        .into_bytes()
    }

    fn children_prefix(&self, parent_ino: u64) -> (Vec<u8>, Vec<u8>) {
        let begin = format!("{}children/{:016x}/", self.prefix, parent_ino).into_bytes();
        let mut end = begin.clone();
        // Increment last byte of prefix to form exclusive upper bound
        // '/' (0x2F) + 1 = '0' (0x30), so range covers all children keys
        if let Some(last) = end.last_mut() {
            *last += 1;
        }
        (begin, end)
    }

    fn next_ino_key(&self) -> Vec<u8> {
        format!("{}next_ino", self.prefix).into_bytes()
    }

    // -----------------------------------------------------------------------
    // Inode operations
    // -----------------------------------------------------------------------

    /// Get inode metadata by ino.
    pub async fn get_inode(&self, ino: u64) -> Result<Option<crate::common::FileMeta>> {
        let key = self.ino_key(ino);

        let result = self
            .db
            .run(|trx, _| {
                let key = key.clone();
                async move { Ok(trx.get(&key, false).await?) }
            })
            .await
            .map_err(|e| RustreError::Fdb(format!("FDB get inode: {e}")))?;

        match result {
            Some(slice) => {
                let meta: crate::common::FileMeta = bincode::deserialize(slice.as_ref())
                    .map_err(|e| RustreError::Serialization(e.to_string()))?;
                Ok(Some(meta))
            }
            None => Ok(None),
        }
    }

    /// Set inode metadata.
    pub async fn set_inode(&self, ino: u64, meta: &crate::common::FileMeta) -> Result<()> {
        let key = self.ino_key(ino);
        let data =
            bincode::serialize(meta).map_err(|e| RustreError::Serialization(e.to_string()))?;

        self.db
            .run(|trx, _| {
                let key = key.clone();
                let data = data.clone();
                async move {
                    trx.set(&key, &data);
                    Ok(())
                }
            })
            .await
            .map_err(|e| RustreError::Fdb(format!("FDB set inode: {e}")))?;
        Ok(())
    }

    /// Delete inode metadata.
    #[allow(dead_code)]
    pub async fn delete_inode(&self, ino: u64) -> Result<()> {
        let key = self.ino_key(ino);

        self.db
            .run(|trx, _| {
                let key = key.clone();
                async move {
                    trx.clear(&key);
                    Ok(())
                }
            })
            .await
            .map_err(|e| RustreError::Fdb(format!("FDB delete inode: {e}")))?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Path → inode mapping
    // -----------------------------------------------------------------------

    /// Resolve a path to an inode number.
    pub async fn resolve_path(&self, path: &str) -> Result<Option<u64>> {
        let key = self.path_key(path);

        let result = self
            .db
            .run(|trx, _| {
                let key = key.clone();
                async move { Ok(trx.get(&key, false).await?) }
            })
            .await
            .map_err(|e| RustreError::Fdb(format!("FDB resolve path: {e}")))?;

        match result {
            Some(slice) => {
                let ino = u64::from_le_bytes(
                    slice
                        .as_ref()
                        .try_into()
                        .map_err(|_| RustreError::Serialization("invalid ino bytes".into()))?,
                );
                Ok(Some(ino))
            }
            None => Ok(None),
        }
    }

    /// Set a path → inode mapping.
    #[allow(dead_code)]
    pub async fn set_path(&self, path: &str, ino: u64) -> Result<()> {
        let key = self.path_key(path);
        let data = ino.to_le_bytes().to_vec();

        self.db
            .run(|trx, _| {
                let key = key.clone();
                let data = data.clone();
                async move {
                    trx.set(&key, &data);
                    Ok(())
                }
            })
            .await
            .map_err(|e| RustreError::Fdb(format!("FDB set path: {e}")))?;
        Ok(())
    }

    /// Delete a path → inode mapping.
    #[allow(dead_code)]
    pub async fn delete_path(&self, path: &str) -> Result<()> {
        let key = self.path_key(path);

        self.db
            .run(|trx, _| {
                let key = key.clone();
                async move {
                    trx.clear(&key);
                    Ok(())
                }
            })
            .await
            .map_err(|e| RustreError::Fdb(format!("FDB delete path: {e}")))?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Children (directory entries)
    // -----------------------------------------------------------------------

    /// Add a child inode to a parent directory.
    #[allow(dead_code)]
    pub async fn add_child(&self, parent_ino: u64, child_ino: u64) -> Result<()> {
        let key = self.child_key(parent_ino, child_ino);

        self.db
            .run(|trx, _| {
                let key = key.clone();
                async move {
                    trx.set(&key, &[]); // empty value — existence is membership
                    Ok(())
                }
            })
            .await
            .map_err(|e| RustreError::Fdb(format!("FDB add child: {e}")))?;
        Ok(())
    }

    /// Remove a child inode from a parent directory.
    #[allow(dead_code)]
    pub async fn remove_child(&self, parent_ino: u64, child_ino: u64) -> Result<()> {
        let key = self.child_key(parent_ino, child_ino);

        self.db
            .run(|trx, _| {
                let key = key.clone();
                async move {
                    trx.clear(&key);
                    Ok(())
                }
            })
            .await
            .map_err(|e| RustreError::Fdb(format!("FDB remove child: {e}")))?;
        Ok(())
    }

    /// List all child inode numbers of a parent directory.
    pub async fn list_children(&self, parent_ino: u64) -> Result<Vec<u64>> {
        let (begin, end) = self.children_prefix(parent_ino);
        let prefix_len = format!("{}children/{:016x}/", self.prefix, parent_ino).len();

        let result = self
            .db
            .run(|trx, _| {
                let begin = begin.clone();
                let end = end.clone();
                async move {
                    let range = trx
                        .get_range(
                            &foundationdb::RangeOption::from((begin.as_slice(), end.as_slice())),
                            1,     // iteration
                            false, // snapshot
                        )
                        .await?;
                    Ok(range)
                }
            })
            .await
            .map_err(|e| RustreError::Fdb(format!("FDB list children: {e}")))?;

        let mut children = Vec::new();
        for kv in result.as_ref() {
            let key_bytes = kv.key();
            // The child ino is the last 16 hex chars of the key
            if key_bytes.len() >= prefix_len + 16 {
                let hex_part = &key_bytes[prefix_len..prefix_len + 16];
                if let Ok(hex_str) = std::str::from_utf8(hex_part) {
                    if let Ok(child_ino) = u64::from_str_radix(hex_str, 16) {
                        children.push(child_ino);
                    }
                }
            }
        }
        Ok(children)
    }

    /// Check if a directory has any children.
    pub async fn has_children(&self, parent_ino: u64) -> Result<bool> {
        let children = self.list_children(parent_ino).await?;
        Ok(!children.is_empty())
    }

    // -----------------------------------------------------------------------
    // Inode counter (atomic allocation across multiple MDS instances)
    // -----------------------------------------------------------------------

    /// Allocate the next inode number atomically.
    /// Uses FDB's atomic add to guarantee uniqueness even with concurrent MDS instances.
    pub async fn alloc_ino(&self) -> Result<u64> {
        let key = self.next_ino_key();

        let ino = self
            .db
            .run(|trx, _| {
                let key = key.clone();
                async move {
                    // Read current value
                    let current = trx.get(&key, false).await?;
                    let ino = match current {
                        Some(slice) => {
                            let bytes: [u8; 8] = slice
                                .as_ref()
                                .try_into()
                                .unwrap_or([2, 0, 0, 0, 0, 0, 0, 0]); // default to 2
                            u64::from_le_bytes(bytes)
                        }
                        None => 2, // Start from 2 (1 = root)
                    };
                    // Write back ino+1
                    let next = (ino + 1).to_le_bytes();
                    trx.set(&key, &next);
                    Ok(ino)
                }
            })
            .await
            .map_err(|e| RustreError::Fdb(format!("FDB alloc ino: {e}")))?;

        Ok(ino)
    }

    // -----------------------------------------------------------------------
    // Transactional compound operations
    // -----------------------------------------------------------------------

    /// Atomically create a file/directory: set inode, path, add to parent's children.
    /// Returns the allocated inode number.
    pub async fn txn_create(
        &self,
        meta: &crate::common::FileMeta,
        path: &str,
        parent_ino: u64,
    ) -> Result<()> {
        let ino_key = self.ino_key(meta.ino);
        let path_key = self.path_key(path);
        let child_key = self.child_key(parent_ino, meta.ino);
        let next_ino_key = self.next_ino_key();
        let next_val = (meta.ino + 1).to_le_bytes().to_vec();

        let meta_data =
            bincode::serialize(meta).map_err(|e| RustreError::Serialization(e.to_string()))?;
        let ino_data = meta.ino.to_le_bytes().to_vec();

        self.db
            .run(|trx, _| {
                let ino_key = ino_key.clone();
                let path_key = path_key.clone();
                let child_key = child_key.clone();
                let next_ino_key = next_ino_key.clone();
                let next_val = next_val.clone();
                let meta_data = meta_data.clone();
                let ino_data = ino_data.clone();
                async move {
                    trx.set(&ino_key, &meta_data);
                    trx.set(&path_key, &ino_data);
                    trx.set(&child_key, &[]); // add child to parent
                    trx.set(&next_ino_key, &next_val); // advance inode counter
                    Ok(())
                }
            })
            .await
            .map_err(|e| RustreError::Fdb(format!("FDB txn_create: {e}")))?;
        Ok(())
    }

    /// Atomically unlink a file/directory: remove inode, path, and parent's child entry.
    /// If `clear_children` is true (for empty dirs), also clear the children sub-range.
    pub async fn txn_unlink(
        &self,
        ino: u64,
        path: &str,
        parent_ino: u64,
        clear_children: bool,
    ) -> Result<()> {
        let ino_key = self.ino_key(ino);
        let path_key = self.path_key(path);
        let child_key = self.child_key(parent_ino, ino);

        // For clearing the directory's children range (should be empty, but clean up)
        let (children_begin, children_end) = if clear_children {
            let (b, e) = self.children_prefix(ino);
            (Some(b), Some(e))
        } else {
            (None, None)
        };

        self.db
            .run(|trx, _| {
                let ino_key = ino_key.clone();
                let path_key = path_key.clone();
                let child_key = child_key.clone();
                let children_begin = children_begin.clone();
                let children_end = children_end.clone();
                async move {
                    trx.clear(&ino_key);
                    trx.clear(&path_key);
                    trx.clear(&child_key);
                    if let (Some(b), Some(e)) = (children_begin, children_end) {
                        trx.clear_range(&b, &e);
                    }
                    Ok(())
                }
            })
            .await
            .map_err(|e| RustreError::Fdb(format!("FDB txn_unlink: {e}")))?;
        Ok(())
    }

    /// Initialize the root directory (ino=1) if it doesn't already exist.
    pub async fn ensure_root(&self) -> Result<()> {
        let root_ino: u64 = 1;

        // Check if root inode already exists
        if (self.get_inode(root_ino).await?).is_some() {
            return Ok(());
        }

        let root = crate::common::FileMeta {
            ino: root_ino,
            name: "/".into(),
            path: "/".into(),
            is_dir: true,
            size: 0,
            ctime: crate::common::FileMeta::now_secs(),
            mtime: crate::common::FileMeta::now_secs(),
            layout: None,
            parent_ino: 0,
        };

        // Atomically set root inode + path + next_ino in one transaction
        let ino_key = self.ino_key(root_ino);
        let path_key = self.path_key("/");
        let next_ino_key = self.next_ino_key();

        let meta_data =
            bincode::serialize(&root).map_err(|e| RustreError::Serialization(e.to_string()))?;
        let ino_data = root_ino.to_le_bytes().to_vec();
        let next_val = 2u64.to_le_bytes().to_vec();

        self.db
            .run(|trx, _| {
                let ino_key = ino_key.clone();
                let path_key = path_key.clone();
                let next_ino_key = next_ino_key.clone();
                let meta_data = meta_data.clone();
                let ino_data = ino_data.clone();
                let next_val = next_val.clone();
                async move {
                    // Only initialize if not already present (idempotent)
                    let existing = trx.get(&ino_key, false).await?;
                    if existing.is_none() {
                        trx.set(&ino_key, &meta_data);
                        trx.set(&path_key, &ino_data);
                        trx.set(&next_ino_key, &next_val);
                    }
                    Ok(())
                }
            })
            .await
            .map_err(|e| RustreError::Fdb(format!("FDB ensure_root: {e}")))?;

        debug!("MDS: initialized root directory in FDB");
        Ok(())
    }
}
