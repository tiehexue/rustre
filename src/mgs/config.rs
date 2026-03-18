//! Cluster configuration management for MGS

use crate::error::Result;
use crate::storage::FdbMetaStore;
use crate::types::{ClusterConfig, MdsInfo, OstInfo};

/// Build a ClusterConfig by reading all MDS and OST entries from FDB.
pub async fn build_cluster_config(store: &FdbMetaStore) -> Result<ClusterConfig> {
    let mds_list: Vec<MdsInfo> = store.list_prefix("mds/").await?;
    let mut ost_list: Vec<OstInfo> = store.list_prefix("ost/").await?;
    // Ensure sorted by ost_index for deterministic striping
    ost_list.sort_by_key(|o| o.ost_index);

    Ok(ClusterConfig { mds_list, ost_list })
}
