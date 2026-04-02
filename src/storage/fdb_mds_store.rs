//! FoundationDB-backed metadata store for MDS

use crate::{error::{Result, RustreError}, rpc::MAX_CHILDREN};
use tracing::debug;

/// FoundationDB-backed metadata store for MDS.
///
/// All filesystem metadata lives in FDB, making MDS completely stateless.
/// Multiple MDS instances can run concurrently against the same FDB cluster.
///
/// Key schema under prefix `{cluster}/mds_meta/`:
///
///   `ino/{ino:016x}`                          → FileMeta (bincode)
///   `path/{normalized_path}`                   → u64 ino (8 bytes LE)
///   `children/{parent_ino:016x}/{name}`        → u64 child_ino (8 bytes LE)
///
/// The children key uses the *name* (not child_ino) as the final component
/// so that hard-links to the same inode under different names in the same
/// directory each have their own child entry.  This is critical for git's
/// link(tmp_pack) → unlink(tmp_pack) workflow.
///
/// Note: Inode allocation is now handled by the MGS inode range allocator.
/// MDS instances request inode ranges in bulk from MGS and allocate locally
/// with zero FDB contention.
///
/// Design rationale:
/// - **Children keyed by name**: each directory entry is `children/{parent}/{name} → ino`;
///   supports hard-links (same ino, different names), single set/clear per add/remove.
/// - **Inode ranges from MGS**: eliminates the `next_ino` FDB hotspot.
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

    /// Child key: `children/{parent_ino:016x}/{name}` → child_ino (u64 LE).
    ///
    /// Using the *name* (not child_ino) as the final key component means a
    /// directory can hold multiple hard-links to the same inode under
    /// different names, which is exactly what `git clone` does:
    ///   link(tmp_pack → final_pack) then unlink(tmp_pack).
    fn child_key(&self, parent_ino: u64, name: &str) -> Vec<u8> {
        format!(
            "{}children/{:016x}/{}",
            self.prefix, parent_ino, name
        )
        .into_bytes()
    }

    fn children_prefix(&self, parent_ino: u64) -> (Vec<u8>, Vec<u8>) {
        let begin = format!("{}children/{:016x}/", self.prefix, parent_ino).into_bytes();
        let mut end = begin.clone();
        // Increment last byte to form an exclusive upper bound for range scan
        if let Some(last) = end.last_mut() {
            *last += 1;
        }
        (begin, end)
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

    /// List all children of a parent directory as (name, child_ino) pairs.
    pub async fn list_children(&self, parent_ino: u64) -> Result<Vec<(String, u64)>> {
        let (begin, end) = self.children_prefix(parent_ino);
        let prefix_len = format!("{}children/{:016x}/", self.prefix, parent_ino).len();

        let result = self
            .db
            .run(|trx, _| {
                let begin = begin.clone();
                let end = end.clone();
                async move {
                    let mut range_opt = foundationdb::RangeOption::from((begin.as_slice(), end.as_slice()));
                    // Set a very large limit to ensure all children are listed in one go
                    range_opt.limit = Some(MAX_CHILDREN);
                    let iteration = (MAX_CHILDREN / 39) + 1;
                    let range = trx
                        .get_range(
                            &range_opt,
                            iteration,     // iteration - with large limit, we only need one iteration
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
            // The name is everything after the prefix
            if key_bytes.len() > prefix_len {
                let name_part = &key_bytes[prefix_len..];
                if let Ok(name) = std::str::from_utf8(name_part) {
                    // Value is child_ino as u64 LE bytes
                    let val = kv.value();
                    if val.len() == 8 {
                        let child_ino = u64::from_le_bytes(
                            val.try_into().unwrap(),
                        );
                        children.push((name.to_string(), child_ino));
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
    // Transactional compound operations
    // -----------------------------------------------------------------------

    /// Atomically create a file/directory: set inode, path, add to parent's children.
    pub async fn txn_create(
        &self,
        meta: &crate::types::FileMeta,
        path: &str,
        parent_ino: u64,
    ) -> Result<()> {
        let ino_key = self.ino_key(meta.ino);
        let path_key = self.path_key(path);
        let child_key = self.child_key(parent_ino, &meta.name);

        let meta_data =
            bincode::serialize(meta).map_err(|e| RustreError::Serialization(e.to_string()))?;
        let ino_data = meta.ino.to_le_bytes().to_vec();

        self.db
            .run(|trx, _| {
                let ino_key = ino_key.clone();
                let path_key = path_key.clone();
                let child_key = child_key.clone();
                let meta_data = meta_data.clone();
                let ino_data = ino_data.clone();
                async move {
                    trx.set(&ino_key, &meta_data);
                    trx.set(&path_key, &ino_data);
                    trx.set(&child_key, &ino_data); // value = child_ino LE bytes
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
        name: &str,
        parent_ino: u64,
        clear_children: bool,
    ) -> Result<()> {
        let ino_key = self.ino_key(ino);
        let path_key = self.path_key(path);
        let child_key = self.child_key(parent_ino, name);

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

    /// Atomically decrement nlink for a hard-linked file: update inode nlink,
    /// remove one path mapping and child entry, but KEEP the inode alive.
    ///
    /// With the name-based child key schema (`children/{parent}/{name}`),
    /// each hard-link name has its own child entry.  Removing the unlinked
    /// name's child entry is safe because the remaining link(s) have their
    /// own entries.
    pub async fn txn_dec_nlink(
        &self,
        ino: u64,
        meta: &crate::types::FileMeta,
        path: &str,
        name: &str,
        parent_ino: u64,
    ) -> Result<()> {
        let ino_key = self.ino_key(ino);
        let path_key = self.path_key(path);
        let child_key = self.child_key(parent_ino, name);

        let meta_data =
            bincode::serialize(meta).map_err(|e| RustreError::Serialization(e.to_string()))?;

        self.db
            .run(|trx, _| {
                let ino_key = ino_key.clone();
                let path_key = path_key.clone();
                let child_key = child_key.clone();
                let meta_data = meta_data.clone();
                async move {
                    // Update inode metadata (decremented nlink)
                    trx.set(&ino_key, &meta_data);
                    // Remove this specific path mapping
                    trx.clear(&path_key);
                    // Remove the child entry for this specific name
                    trx.clear(&child_key);
                    Ok(())
                }
            })
            .await
            .map_err(|e| RustreError::Fdb(format!("FDB txn_dec_nlink: {e}")))?;
        Ok(())
    }

    /// Atomically rename a file/directory: update inode metadata, path mapping, and parent-child links.
    pub async fn txn_rename(
        &self,
        ino: u64,
        old_path: &str,
        new_path: &str,
        old_name: &str,
        new_name: &str,
        old_parent_ino: u64,
        new_parent_ino: u64,
        meta: &crate::types::FileMeta,
    ) -> Result<()> {
        let ino_key = self.ino_key(ino);
        let old_path_key = self.path_key(old_path);
        let new_path_key = self.path_key(new_path);
        let old_child_key = self.child_key(old_parent_ino, old_name);
        let new_child_key = self.child_key(new_parent_ino, new_name);

        let meta_data =
            bincode::serialize(meta).map_err(|e| RustreError::Serialization(e.to_string()))?;
        let ino_data = ino.to_le_bytes().to_vec();

        self.db
            .run(|trx, _| {
                let ino_key = ino_key.clone();
                let old_path_key = old_path_key.clone();
                let new_path_key = new_path_key.clone();
                let old_child_key = old_child_key.clone();
                let new_child_key = new_child_key.clone();
                let meta_data = meta_data.clone();
                let ino_data = ino_data.clone();
                async move {
                    // Update inode metadata
                    trx.set(&ino_key, &meta_data);
                    // Update path mappings
                    trx.clear(&old_path_key);
                    trx.set(&new_path_key, &ino_data);
                    // Update parent-child relationships
                    trx.clear(&old_child_key);
                    trx.set(&new_child_key, &ino_data);
                    Ok(())
                }
            })
            .await
            .map_err(|e| RustreError::Fdb(format!("FDB txn_rename: {e}")))?;
        Ok(())
    }

    /// Atomically create a hard link: update inode metadata (nlink), add path mapping, add parent-child entry.
    pub async fn txn_link(
        &self,
        ino: u64,
        meta: &crate::types::FileMeta,
        new_path: &str,
        new_name: &str,
        new_parent_ino: u64,
    ) -> Result<()> {
        let ino_key = self.ino_key(ino);
        let path_key = self.path_key(new_path);
        let child_key = self.child_key(new_parent_ino, new_name);

        let meta_data =
            bincode::serialize(meta).map_err(|e| RustreError::Serialization(e.to_string()))?;
        let ino_data = ino.to_le_bytes().to_vec();

        self.db
            .run(|trx, _| {
                let ino_key = ino_key.clone();
                let path_key = path_key.clone();
                let child_key = child_key.clone();
                let meta_data = meta_data.clone();
                let ino_data = ino_data.clone();
                async move {
                    trx.set(&ino_key, &meta_data);
                    trx.set(&path_key, &ino_data);
                    trx.set(&child_key, &ino_data);
                    Ok(())
                }
            })
            .await
            .map_err(|e| RustreError::Fdb(format!("FDB txn_link: {e}")))?;
        Ok(())
    }

    /// Initialize the root directory (ino=1) if it doesn't already exist.
    /// Note: Does NOT initialize the inode counter — that is managed by MGS.
    pub async fn ensure_root(&self) -> Result<()> {
        let root_ino: u64 = 1;

        // Check if root inode already exists
        if (self.get_inode(root_ino).await?).is_some() {
            return Ok(());
        }

        let (uid, gid) = crate::utils::fid::get_ugid();

        let root = crate::types::FileMeta {
            ino: root_ino,
            name: "/".into(),
            path: "/".into(),
            is_dir: true,
            size: 0,
            ctime: crate::types::FileMeta::now_secs(),
            mtime: crate::types::FileMeta::now_secs(),
            mode: 0o755,
            uid,
            gid,
            layout: None,
            parent_ino: 0,
            pending: false,
            nlink: 2, // root dir: . and parent
            symlink_target: None,
        };

        // Atomically set root inode + path in one transaction
        let ino_key = self.ino_key(root_ino);
        let path_key = self.path_key("/");

        let meta_data =
            bincode::serialize(&root).map_err(|e| RustreError::Serialization(e.to_string()))?;
        let ino_data = root_ino.to_le_bytes().to_vec();

        self.db
            .run(|trx, _| {
                let ino_key = ino_key.clone();
                let path_key = path_key.clone();
                let meta_data = meta_data.clone();
                let ino_data = ino_data.clone();
                async move {
                    // Only initialize if not already present (idempotent)
                    let existing = trx.get(&ino_key, false).await?;
                    if existing.is_none() {
                        trx.set(&ino_key, &meta_data);
                        trx.set(&path_key, &ino_data);
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
