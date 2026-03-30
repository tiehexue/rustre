//! PUT command implementation for Rustre client

use crate::client::operations::{get_config, mds_addr};
use crate::error::{Result, RustreError};
use crate::rpc::{rpc_call, RpcKind};
use crate::types::CreateReq;
use futures::future::join_all;
use std::path::Path;
use tracing::{error, info, warn};

/// PUT command implementation
pub async fn cmd_put(
    mgs_addr: &str,
    source: &str,
    dest: &str,
    stripe_count: u32,
    stripe_size: u64,
    replica_count: u32,
) -> Result<()> {
    let metadata = tokio::fs::metadata(source).await.map_err(|e| {
        RustreError::Io(std::io::Error::new(
            e.kind(),
            format!("getting metadata for {source}: {e}"),
        ))
    })?;

    if metadata.is_dir() {
        put_directory(
            mgs_addr,
            source,
            dest,
            stripe_count,
            stripe_size,
            replica_count,
        )
        .await
    } else {
        put_file(
            mgs_addr,
            source,
            dest,
            stripe_count,
            stripe_size,
            replica_count,
        )
        .await
    }
}

async fn put_file(
    mgs_addr: &str,
    source: &str,
    dest: &str,
    stripe_count: u32,
    stripe_size: u64,
    replica_count: u32,
) -> Result<()> {
    // Get file size first
    let file_size = tokio::fs::metadata(source)
        .await
        .map_err(|e| {
            RustreError::Io(std::io::Error::new(
                e.kind(),
                format!("getting metadata for {source}: {e}"),
            ))
        })?
        .len();
    info!("PUT: {source} → {dest} ({file_size} bytes)");

    // Fetch cluster config
    let config = get_config(mgs_addr).await?;
    let mds = mds_addr(&config)?;

    // ── Phase 1: Create pending file on MDS ──
    // The file is created with pending=true, invisible to other clients.
    let (uid, gid) = crate::utils::fid::get_ugid();

    let mode = 0o644; // Default file mode
    let reply = rpc_call(
        &mds,
        RpcKind::Create(CreateReq {
            path: dest.to_string(),
            stripe_count,
            stripe_size,
            replica_count,
            mode,
            uid,
            gid,
        }),
    )
    .await?;

    let meta = match reply.kind {
        RpcKind::MetaReply(m) => m,
        RpcKind::Error(e) => return Err(RustreError::Net(e)),
        _ => return Err(RustreError::Net("unexpected reply from MDS".into())),
    };

    let layout = meta
        .layout
        .as_ref()
        .ok_or_else(|| RustreError::Internal("no stripe layout returned".into()))?;

    println!(
        "Created {dest} (ino={}, stripes={}, stripe_size={}, replica_count={}, pending)",
        meta.ino,
        layout.ost_indices.len(),
        layout.stripe_size,
        layout.replica_count
    );

    // ── Phase 2: Write data to OSS ──
    // One task per stripe, all run in parallel.
    let mut write_futures = Vec::new();
    for ost_assignment in 0..layout.ost_indices.len() as u32 {
        let source_path = source.to_string();
        let config_clone = config.clone();
        let layout_clone = layout.clone();
        let meta_ino = meta.ino;

        write_futures.push(tokio::spawn(async move {
            crate::zerocopy::transfer::ost_zerocopy_task(
                source_path,
                meta_ino,
                layout_clone,
                config_clone,
                ost_assignment,
            )
            .await
        }));
    }

    // Wait for all writes to complete
    let results = join_all(write_futures).await;
    let mut write_error: Option<RustreError> = None;
    for (i, res) in results.into_iter().enumerate() {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                error!("OST writer task {i} failed: {e}");
                if write_error.is_none() {
                    write_error = Some(e);
                }
            }
            Err(e) => {
                error!("OST writer task {i} panicked: {e}");
                if write_error.is_none() {
                    write_error = Some(RustreError::Internal(format!("write task panicked: {e}")));
                }
            }
        }
    }

    // ── Phase 2 failed: abort and clean up ──
    if let Some(err) = write_error {
        warn!("PUT: write failed for {dest}, aborting pending file and cleaning up OSS objects");

        // Best-effort: tell MDS to remove the pending record
        if let Err(e) = rpc_call(&mds, RpcKind::AbortCreate { ino: meta.ino }).await {
            warn!("PUT: failed to abort pending MDS record for {dest}: {e}");
        }

        // Best-effort: delete any objects that were already written to OSTs
        cleanup_oss_objects(&config, meta.ino).await;

        return Err(err);
    }

    // ── Phase 3: Commit — set size and make file visible ──
    let commit_reply = rpc_call(
        &mds,
        RpcKind::CommitCreate {
            ino: meta.ino,
            size: file_size,
        },
    )
    .await?;

    match commit_reply.kind {
        RpcKind::Ok => {}
        RpcKind::Error(e) => {
            // Commit failed after all data was written — this is the worst case.
            // The data exists on OSS but the file remains pending (invisible).
            // A retry of CommitCreate could recover this, or the pending GC will clean it up.
            error!("PUT: commit failed for {dest}: {e}");
            return Err(RustreError::Net(format!("commit failed: {e}")));
        }
        _ => {
            return Err(RustreError::Net("unexpected reply from MDS".into()));
        }
    }

    println!(
        "Successfully wrote {file_size} bytes to {dest} across {} OSTs",
        layout.ost_indices.len()
    );
    Ok(())
}

/// Best-effort cleanup: delete all objects for the given inode from all OSTs.
/// Errors are logged but not propagated — this is crash-recovery territory.
async fn cleanup_oss_objects(config: &crate::types::ClusterConfig, ino: u64) {
    let mut delete_futures = Vec::new();
    for ost_info in &config.ost_list {
        let addr = ost_info.address.clone();
        delete_futures.push(tokio::spawn(async move {
            match rpc_call(&addr, RpcKind::ObjDeleteInode { ino }).await {
                Ok(_) => {}
                Err(e) => {
                    warn!("cleanup: failed to delete objects for ino={ino:016x} from {addr}: {e}");
                }
            }
        }));
    }
    join_all(delete_futures).await;
}

async fn put_directory(
    mgs_addr: &str,
    source: &str,
    dest: &str,
    stripe_count: u32,
    stripe_size: u64,
    replica_count: u32,
) -> Result<()> {
    use crate::client::operations::cmd_mkdir;

    let source_path = Path::new(source);

    async fn walk_and_put(
        mgs_addr: &str,
        source_root: &Path,
        dest_root: &str,
        current_source: &Path,
        stripe_count: u32,
        stripe_size: u64,
        replica_count: u32,
    ) -> Result<()> {
        let mut entries = tokio::fs::read_dir(current_source).await.map_err(|e| {
            RustreError::Io(std::io::Error::new(
                e.kind(),
                format!("reading directory {}: {e}", current_source.display()),
            ))
        })?;

        while let Some(entry) = entries.next_entry().await.map_err(|e| {
            RustreError::Io(std::io::Error::new(
                e.kind(),
                format!(
                    "reading directory entry in {}: {e}",
                    current_source.display()
                ),
            ))
        })? {
            let entry_path = entry.path();
            let relative_path = entry_path.strip_prefix(source_root).map_err(|_| {
                RustreError::Internal(format!(
                    "failed to compute relative path for {}",
                    entry_path.display()
                ))
            })?;
            let dest_entry_path = Path::new(dest_root).join(relative_path);

            let metadata = tokio::fs::metadata(&entry_path).await.map_err(|e| {
                RustreError::Io(std::io::Error::new(
                    e.kind(),
                    format!("getting metadata for {}: {e}", entry_path.display()),
                ))
            })?;

            if metadata.is_dir() {
                // Create directory on remote
                cmd_mkdir(mgs_addr, &dest_entry_path.to_string_lossy()).await?;
                // Recurse
                Box::pin(walk_and_put(
                    mgs_addr,
                    source_root,
                    dest_root,
                    &entry_path,
                    stripe_count,
                    stripe_size,
                    replica_count,
                ))
                .await?;
            } else {
                // Put file
                put_file(
                    mgs_addr,
                    &entry_path.to_string_lossy(),
                    &dest_entry_path.to_string_lossy(),
                    stripe_count,
                    stripe_size,
                    replica_count,
                )
                .await?;
            }
        }
        Ok(())
    }

    // Create the root destination directory if it doesn't exist
    cmd_mkdir(mgs_addr, dest).await?;
    // Walk and put
    walk_and_put(
        mgs_addr,
        source_path,
        dest,
        source_path,
        stripe_count,
        stripe_size,
        replica_count,
    )
    .await
}
