//! Client status display

use crate::error::{Result, RustreError};
use crate::rpc::{rpc_call, RpcKind};

/// Display cluster status from MGS
pub async fn status(mgs_addr: &str) -> Result<()> {
    let reply = rpc_call(mgs_addr, RpcKind::GetStatus).await?;
    match reply.kind {
        RpcKind::StatusReply(info) => {
            println!("╔══════════════════════════════════════════════╗");
            println!("║          Rustre Cluster Status               ║");
            println!("╠══════════════════════════════════════════════╣");
            println!("║  MDS servers: {:<31}║", info.mds_count);
            for mds in &info.mds_list {
                println!("║    └─ {:<39}║", mds.address);
            }
            println!("║  OST targets: {:<31}║", info.ost_count);
            for ost in &info.osts {
                let used_mb = ost.used_bytes / (1024 * 1024);
                let total_gb = ost.total_bytes / (1024 * 1024 * 1024);
                println!(
                    "║    └─ OST-{}: {} ({} MB / {} GB) ║",
                    ost.ost_index, ost.address, used_mb, total_gb
                );
            }
            println!("╚══════════════════════════════════════════════╝");
            Ok(())
        }
        RpcKind::Error(e) => {
            eprintln!("Error: {e}");
            Err(RustreError::Net(e))
        }
        _ => {
            eprintln!("Unexpected reply from MGS");
            Err(RustreError::Net("unexpected reply".into()))
        }
    }
}
