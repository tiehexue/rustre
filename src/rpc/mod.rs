//! Length-prefixed bincode RPC over TCP.
//!
//! Wire format: `[4-byte BE length][bincode payload]`
//!
//! Analogous to Lustre's PTLRPC + LNet, simplified for userspace TCP.

use crate::error::{Result, RustreError};
use crate::types::{ClusterConfig, CreateReq, FileMeta, MdsInfo, OstInfo, StatusInfo};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::trace;

pub(crate) static MSG_COUNTER: AtomicU64 = AtomicU64::new(1);

// ---------------------------------------------------------------------------
// RPC messages — the wire protocol between all components
// ---------------------------------------------------------------------------

/// Every message on the wire is a `RpcMessage` serialised with bincode,
/// length-prefixed with a 4-byte big-endian u32.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcMessage {
    pub id: u64,
    pub kind: RpcKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RpcKind {
    // -- MGS --
    RegisterMds(MdsInfo),
    RegisterOst(OstInfo),
    GetConfig,
    UpdateOstUsage {
        ost_index: u32,
        used_bytes: u64,
    },

    // -- MDS --
    Lookup(String),
    Create(CreateReq),
    Mkdir(String),
    Readdir(String),
    Unlink(String),
    Stat(String),
    SetSize {
        path: String,
        size: u64,
    },
    Rename {
        old_path: String,
        new_path: String,
    },

    // -- OSS (zero-copy: data follows the RPC header on the wire) --
    ObjWriteZeroCopy {
        object_id: String,
        length: usize,
    },
    ObjReadZeroCopy {
        object_id: String,
        length: usize,
    },
    ObjDelete {
        object_id: String,
    },
    /// Bulk-delete all objects for an inode (prefix scan on OST).
    ObjDeleteInode {
        ino: u64,
    },

    // -- Replies --
    Ok,
    Error(String),
    MetaReply(FileMeta),
    MetaListReply(Vec<FileMeta>),
    DataReply(Vec<u8>),
    ConfigReply(ClusterConfig),
    StatusReply(StatusInfo),

    // -- Status + Heartbeat --
    // NOTE: variant order is load-bearing — bincode uses ordinals on the wire.
    // New variants MUST be appended at the end to preserve wire compatibility.
    GetStatus,
    Heartbeat,
    HeartbeatReply,

    // -- Two-phase commit for data integrity --
    /// Commit a pending file: set final size and mark as visible.
    /// Sent by client after all OSS writes succeed.
    /// Uses ino directly — no redundant path resolution.
    CommitCreate {
        ino: u64,
        size: u64,
    },
    /// Abort a pending file: remove the MDS record.
    /// Sent by client if OSS writes fail (best-effort cleanup).
    AbortCreate {
        ino: u64,
    },

    // -- Inode range allocator --
    /// MDS requests a batch of inode numbers from MGS to avoid per-file FDB contention.
    /// The only inode-range operation — no return/reclaim protocol needed.
    /// u64 inode space (~1.8×10¹⁹) is effectively infinite; leaked ranges are harmless.
    AllocInodeRange {
        count: u64,
    },
    /// MGS reply: the allocated inode range [start, end) — `end` is exclusive.
    InodeRangeReply {
        start: u64,
        end: u64,
    },
    /// Kept for wire compatibility (bincode ordinals). No longer used.
    #[doc(hidden)]
    ReturnInodeRange {
        start: u64,
        end: u64,
    },
}

// ---------------------------------------------------------------------------
// Networking functions
// ---------------------------------------------------------------------------

/// Send an RPC message over a TCP stream.
pub async fn send_msg(stream: &mut TcpStream, msg: &RpcMessage) -> Result<()> {
    let payload = bincode::serialize(msg).map_err(|e| RustreError::Serialization(e.to_string()))?;
    let len = payload.len() as u32;
    stream
        .write_all(&len.to_be_bytes())
        .await
        .map_err(|e| RustreError::Net(e.to_string()))?;
    stream
        .write_all(&payload)
        .await
        .map_err(|e| RustreError::Net(e.to_string()))?;
    stream
        .flush()
        .await
        .map_err(|e| RustreError::Net(e.to_string()))?;
    trace!(
        "sent msg id={} kind={:?} ({} bytes)",
        msg.id,
        std::mem::discriminant(&msg.kind),
        len
    );
    Ok(())
}

/// Receive an RPC message from a TCP stream.
pub async fn recv_msg(stream: &mut TcpStream) -> Result<RpcMessage> {
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| RustreError::Net(format!("read length: {e}")))?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    stream
        .read_exact(&mut buf)
        .await
        .map_err(|e| RustreError::Net(format!("read payload: {e}")))?;
    let msg: RpcMessage =
        bincode::deserialize(&buf).map_err(|e| RustreError::Serialization(e.to_string()))?;
    trace!("recv msg id={} ({} bytes)", msg.id, len);
    Ok(msg)
}

/// Convenience: connect to addr, send a request, receive reply.
pub async fn rpc_call(addr: &str, kind: RpcKind) -> Result<RpcMessage> {
    let mut stream = TcpStream::connect(addr)
        .await
        .map_err(|e| RustreError::Net(format!("connect to {addr}: {e}")))?;
    let msg = RpcMessage {
        id: MSG_COUNTER.fetch_add(1, Ordering::Relaxed),
        kind,
    };
    send_msg(&mut stream, &msg).await?;
    let reply = recv_msg(&mut stream).await?;
    trace!("rpc_call to {addr} done, reply id={}", reply.id);
    Ok(reply)
}

/// Make an RPC reply with the same id as the request.
pub fn make_reply(req_id: u64, kind: RpcKind) -> RpcMessage {
    RpcMessage { id: req_id, kind }
}
