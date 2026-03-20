//! RM — remove file (uses bulk ObjDeleteInode for efficient cleanup)

use crate::client::operations::{get_config, mds_addr};
use crate::error::{Result, RustreError};
use crate::rpc::{rpc_call, RpcKind};
use futures::future::join_all;

/// Remove a file from the cluster.
///
/// This function:
/// 1. Gets file metadata from MDS to determine which OSTs hold stripe objects
/// 2. Sends bulk ObjDeleteInode requests to all OSTs for efficient cleanup
/// 3. Removes the file entry from MDS
pub async fn cmd_rm(mgs_addr: &str, path: &str) -> Result<()> {
    let config = get_config(mgs_addr).await?;
    let mds = mds_addr(&config)?;

    // First get the file metadata to know which OSTs to notify
    let stat_reply = rpc_call(&mds, RpcKind::Stat(path.to_string())).await?;
    let meta = match stat_reply.kind {
        RpcKind::MetaReply(m) => m,
        RpcKind::Error(e) => return Err(RustreError::Net(e)),
        _ => return Err(RustreError::Net("unexpected reply".into())),
    };

    // Delete all stripe objects from OSTs using bulk inode deletion.
    // We send ObjDeleteInode to all OSTs that could hold stripes for this file.
    if meta.layout.is_some() {
        // Send bulk delete to all OSTs in the cluster (they'll handle prefix scan)
        let mut delete_futures = Vec::new();
        for ost_info in &config.ost_list {
            let addr = ost_info.address.clone();
            let ino = meta.ino;
            delete_futures.push(tokio::spawn(async move {
                let _ = rpc_call(&addr, RpcKind::ObjDeleteInode { ino }).await;
            }));
        }
        join_all(delete_futures).await;
    }

    // Remove from MDS
    let reply = rpc_call(&mds, RpcKind::Unlink(path.to_string())).await?;
    match reply.kind {
        RpcKind::Ok => {
            println!("Removed: {path}");
            Ok(())
        }
        RpcKind::Error(e) => {
            eprintln!("Error: {e}");
            Err(RustreError::Net(e))
        }
        _ => Err(RustreError::Net("unexpected reply".into())),
    }
}
