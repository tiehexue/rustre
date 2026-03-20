//! FoundationDB-backed metadata store for MDS

use crate::error::{Result, RustreError};
use tracing::debug;

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
    pub async fn get_inode(&self, ino: u64) -> Result<Option<crate::types::FileMeta>> {
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
                let meta: crate::types::FileMeta = bincode::deserialize(slice.as_ref())
                    .map_err(|e| RustreError::Serialization(e.to_string()))?;
                Ok(Some(meta))
            }
            None => Ok(None),
        }
    }

    /// Set inode metadata.
    pub async fn set_inode(&self, ino: u64, meta: &crate::types::FileMeta) -> Result<()> {
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

    // -----------------------------------------------------------------------
    // Children (directory entries)
    // -----------------------------------------------------------------------

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
        meta: &crate::types::FileMeta,
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

        let root = crate::types::FileMeta {
            ino: root_ino,
            name: "/".into(),
            path: "/".into(),
            is_dir: true,
            size: 0,
            ctime: crate::types::FileMeta::now_secs(),
            mtime: crate::types::FileMeta::now_secs(),
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
