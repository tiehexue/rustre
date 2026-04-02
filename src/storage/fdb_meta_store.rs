//! FoundationDB-backed metadata store for MGS

use crate::{
    error::{Result, RustreError},
    rpc::MAX_CHILDREN,
};

/// FoundationDB-backed key-value store for MGS cluster state.
///
/// Key schema under prefix `{cluster}/`:
///   {cluster}/mds/{address}           → MdsInfo (bincode)
///   {cluster}/ost/{ost_index:08x}     → OstInfo (bincode)
///   {cluster}/ost_usage/{index:08x}   → u64 used_bytes (bincode)
///   {cluster}/next_ino_range          → u64 (8 bytes LE, global inode counter)
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
            .run(|trx, _| {
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
            .run(|trx, _| {
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
    pub async fn delete(&self, suffix: &str) -> Result<()> {
        let key = self.key(suffix);

        self.db
            .run(|trx, _| {
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
            .run(|trx, _| {
                let begin = begin.clone();
                let end = end.clone();
                async move {
                    let mut range_opt =
                        foundationdb::RangeOption::from((begin.as_slice(), end.as_slice()));
                    // Set a very large limit to ensure all items are listed in one go
                    range_opt.limit = Some(MAX_CHILDREN);
                    let iteration = (MAX_CHILDREN / 39) + 1;
                    let range = trx
                        .get_range(
                            &range_opt,
                            iteration, // iteration - with large limit, we only need one iteration
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

    // -----------------------------------------------------------------------
    // Inode range allocator (centralized in MGS)
    // -----------------------------------------------------------------------

    fn next_ino_range_key(&self) -> Vec<u8> {
        format!("{}next_ino_range", self.prefix).into_bytes()
    }

    /// Atomically allocate a range of `count` inode numbers.
    /// Returns `(start, end)` where the range is `[start, end)`.
    ///
    /// This is the global inode counter managed by MGS. MDS instances
    /// request ranges in bulk to avoid per-file FDB contention.
    ///
    /// The counter only ever advances — no reclaim protocol.
    /// u64 inode space (~1.8×10¹⁹) is effectively infinite.
    pub async fn alloc_inode_range(&self, count: u64) -> Result<(u64, u64)> {
        let key = self.next_ino_range_key();

        let (start, end) = self
            .db
            .run(|trx, _| {
                let key = key.clone();
                async move {
                    let current = trx.get(&key, false).await?;
                    let start = match current {
                        Some(slice) => {
                            let bytes: [u8; 8] = slice
                                .as_ref()
                                .try_into()
                                .unwrap_or([2, 0, 0, 0, 0, 0, 0, 0]);
                            u64::from_le_bytes(bytes)
                        }
                        None => 2, // Start from 2 (ino 1 = root)
                    };
                    let end = start + count;
                    trx.set(&key, &end.to_le_bytes());
                    Ok((start, end))
                }
            })
            .await
            .map_err(|e| RustreError::Fdb(format!("FDB alloc_inode_range: {e}")))?;

        Ok((start, end))
    }
}
