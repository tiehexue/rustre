//! RPC protocol definitions and networking layer.
//!
//! Networking layer — length-prefixed bincode RPC over TCP.
//! Analogous to Lustre's PTLRPC + LNet, but simplified for userspace TCP.
//! Wire format: [4-byte big-endian length][bincode payload]

use crate::error::{Result, RustreError};
use crate::types::{
    ClusterConfig, CreateReq, FileMeta, MdsInfo, ObjReadReq, ObjWriteReq, OstInfo, StatusInfo,
};
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
    // -- MGS RPCs --
    RegisterMds(MdsInfo),
    RegisterOst(OstInfo),
    GetConfig,
    GetConfigReply(ClusterConfig),
    UpdateOstUsage {
        ost_index: u32,
        used_bytes: u64,
    },

    // -- MDS RPCs --
    Lookup(String),    // path → FileMeta
    Create(CreateReq), // create file, returns FileMeta with layout
    Mkdir(String),     // create directory
    Readdir(String),   // list directory → Vec<FileMeta>
    Unlink(String),    // remove file
    Stat(String),      // stat → FileMeta
    SetSize {
        path: String,
        size: u64,
    },

    // -- OSS RPCs --
    ObjWrite(ObjWriteReq),
    ObjRead(ObjReadReq),
    /// Zero-copy object write request (metadata only, data follows via sendfile)
    ObjWriteZeroCopy {
        object_id: String,
        length: usize,
    },
    ObjDelete {
        object_id: String,
    },
    /// Delete all objects for an inode (bulk cleanup)
    ObjDeleteInode {
        ino: u64,
    },

    // -- Generic replies --
    Ok,
    Error(String),
    MetaReply(FileMeta),
    MetaListReply(Vec<FileMeta>),
    DataReply(Vec<u8>),
    ConfigReply(ClusterConfig),
    StatusReply(StatusInfo),

    // -- Status --
    GetStatus,

    // -- Heartbeat --
    Heartbeat,
    HeartbeatReply,
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
