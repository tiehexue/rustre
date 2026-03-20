//! RM — remove file/directory with data integrity guarantees.
//!
//! Delete order: **MDS first, then OSS data**.
//!
//! Rationale: if MDS Unlink succeeds but OSS cleanup fails, we have orphaned
//! objects on disk (harmless — they waste space but are never referenced).
//! The reverse order (OSS first, then MDS) is dangerous: if MDS Unlink fails
//! after OSS data is deleted, the file metadata still exists but points to
//! nothing — every subsequent GET will fail with confusing errors.

use crate::client::operations::{get_config, mds_addr};
use crate::error::{Result, RustreError};
use crate::rpc::{rpc_call, RpcKind};
use futures::future::join_all;
use tracing::warn;

/// Remove a file or empty directory from the cluster.
///
/// Protocol:
/// 1. Stat the file to get metadata (ino, layout)
/// 2. Unlink from MDS first (atomic FDB transaction) — file becomes invisible
/// 3. Delete stripe objects from all OSTs (best-effort, errors logged)
pub async fn cmd_rm(mgs_addr: &str, path: &str) -> Result<()> {
    let config = get_config(mgs_addr).await?;
    let mds = mds_addr(&config)?;

    // Step 1: Get file metadata to know the inode and whether it has stripe data
    let stat_reply = rpc_call(&mds, RpcKind::Stat(path.to_string())).await?;
    let meta = match stat_reply.kind {
        RpcKind::MetaReply(m) => m,
        RpcKind::Error(e) => return Err(RustreError::Net(e)),
        _ => return Err(RustreError::Net("unexpected reply".into())),
    };

    let has_objects = meta.layout.is_some();
    let ino = meta.ino;

    // Step 2: Unlink from MDS FIRST — the file becomes invisible immediately.
    // If this fails, we stop — no data has been touched.
    let reply = rpc_call(&mds, RpcKind::Unlink(path.to_string())).await?;
    match reply.kind {
        RpcKind::Ok => {}
        RpcKind::Error(e) => {
            eprintln!("Error: {e}");
            return Err(RustreError::Net(e));
        }
        _ => return Err(RustreError::Net("unexpected reply".into())),
    }

    // Step 3: Delete stripe objects from all OSTs (best-effort).
    // The MDS record is already gone, so even if some OSTs are unreachable,
    // the worst case is orphaned objects that waste disk space — never
    // a phantom file that references deleted data.
    if has_objects {
        let mut delete_futures = Vec::new();
        for ost_info in &config.ost_list {
            let addr = ost_info.address.clone();
            delete_futures.push(tokio::spawn(async move {
                match rpc_call(&addr, RpcKind::ObjDeleteInode { ino }).await {
                    Ok(reply) => match reply.kind {
                        RpcKind::Ok => {}
                        RpcKind::Error(e) => {
                            warn!(
                                "RM: OSS at {addr} failed to delete objects for ino={ino:016x}: {e}"
                            );
                        }
                        _ => {
                            warn!("RM: unexpected reply from OSS {addr} during object cleanup");
                        }
                    },
                    Err(e) => {
                        warn!("RM: could not reach OSS {addr} for cleanup of ino={ino:016x}: {e}");
                    }
                }
            }));
        }
        join_all(delete_futures).await;
    }

    println!("Removed: {path}");
    Ok(())
}
