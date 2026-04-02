//! OST (Object Storage Target) I/O helpers for the FUSE mount.
//!
//! Provides async read/write primitives that the filesystem callbacks use
//! (via `block_on`) to move data between OSTs and FUSE buffers.

use crate::client::operations::ost_addr;
use crate::rpc::{recv_msg, send_msg, RpcKind, RpcMessage, MSG_COUNTER};
use crate::types::{FileMeta, StripeLayout};
use fuser::Errno;
use std::sync::atomic::Ordering;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tracing::{debug, error};

use super::helpers::rustre_err_to_errno;
use super::types::RustreFs;

// ---------------------------------------------------------------------------
// Low-level chunk I/O
// ---------------------------------------------------------------------------

/// Read an entire chunk from an OST.
pub async fn read_chunk_from_ost(addr: &str, object_id: &str) -> Result<Vec<u8>, Errno> {
    let mut stream = TcpStream::connect(addr).await.map_err(|_| Errno::EIO)?;

    let request = RpcMessage {
        id: MSG_COUNTER.fetch_add(1, Ordering::Relaxed),
        kind: RpcKind::ObjReadZeroCopy {
            object_id: object_id.to_string(),
            length: 0, // 0 = read entire object
        },
    };
    send_msg(&mut stream, &request)
        .await
        .map_err(|_| Errno::EIO)?;

    let reply = recv_msg(&mut stream).await.map_err(|_| Errno::EIO)?;

    match reply.kind {
        RpcKind::DataReply(length_bytes) => {
            let bytes: [u8; 8] = length_bytes.as_slice().try_into().map_err(|_| Errno::EIO)?;
            let length = usize::from_le_bytes(bytes);
            let mut data = vec![0u8; length];
            stream.read_exact(&mut data).await.map_err(|_| Errno::EIO)?;
            Ok(data)
        }
        RpcKind::Error(e) => {
            if e.contains("not found") {
                // Chunk doesn't exist — return zeros (sparse file support)
                Ok(Vec::new())
            } else {
                Err(Errno::EIO)
            }
        }
        _ => Err(Errno::EIO),
    }
}

/// Write a chunk to an OST.
pub async fn write_chunk_to_ost(addr: &str, object_id: &str, data: &[u8]) -> Result<(), Errno> {
    let mut stream = TcpStream::connect(addr).await.map_err(|_| Errno::EIO)?;

    let request = RpcMessage {
        id: MSG_COUNTER.fetch_add(1, Ordering::Relaxed),
        kind: RpcKind::ObjWriteZeroCopy {
            object_id: object_id.to_string(),
            length: data.len(),
        },
    };
    send_msg(&mut stream, &request)
        .await
        .map_err(|_| Errno::EIO)?;

    // Send data after the RPC header
    use tokio::io::AsyncWriteExt;
    stream.write_all(data).await.map_err(|_| Errno::EIO)?;
    stream.flush().await.map_err(|_| Errno::EIO)?;

    let reply = recv_msg(&mut stream).await.map_err(|_| Errno::EIO)?;
    match reply.kind {
        RpcKind::Ok => Ok(()),
        RpcKind::Error(e) => {
            error!("OST write failed: {e}");
            Err(Errno::EIO)
        }
        _ => Err(Errno::EIO),
    }
}

// ---------------------------------------------------------------------------
// High-level read/write for FUSE
// ---------------------------------------------------------------------------

impl RustreFs {
    /// Read data from OSTs for a specific byte range of a file.
    pub fn read_data(&self, meta: &FileMeta, offset: u64, size: u32) -> Result<Vec<u8>, Errno> {
        let layout = meta.layout.as_ref().ok_or(Errno::EIO)?;
        let file_size = meta.size;
        let config = self.snap_config();

        // Clamp read to file bounds
        if offset >= file_size {
            return Ok(Vec::new());
        }
        let read_end = std::cmp::min(offset + size as u64, file_size);
        let read_len = (read_end - offset) as usize;

        let chunk_size = layout.stripe_size;

        // Determine which chunks we need
        let first_chunk = (offset / chunk_size) as u32;
        let last_chunk = ((read_end - 1) / chunk_size) as u32;

        let mut result = vec![0u8; read_len];

        self.rt.block_on(async {
            let mut read_handles = Vec::new();

            for chunk_idx in first_chunk..=last_chunk {
                let chunk_file_offset = chunk_idx as u64 * chunk_size;
                let chunk_end = std::cmp::min(chunk_file_offset + chunk_size, file_size);

                // What portion of this chunk do we need?
                let read_start_in_chunk = if offset > chunk_file_offset {
                    (offset - chunk_file_offset) as usize
                } else {
                    0
                };
                let read_end_in_chunk = if read_end < chunk_end {
                    (read_end - chunk_file_offset) as usize
                } else {
                    (chunk_end - chunk_file_offset) as usize
                };

                // Where does this go in our result buffer?
                let buf_start = if chunk_file_offset > offset {
                    (chunk_file_offset - offset) as usize
                } else {
                    0
                };

                let object_id = StripeLayout::object_id(meta.ino, chunk_idx);
                let primary_ost = layout.ost_for_chunk(chunk_idx);
                let addr = ost_addr(&config, primary_ost).map_err(rustre_err_to_errno)?;

                read_handles.push((
                    chunk_idx,
                    buf_start,
                    read_start_in_chunk,
                    read_end_in_chunk,
                    tokio::spawn(async move { read_chunk_from_ost(&addr, &object_id).await }),
                ));
            }

            for (chunk_idx, buf_start, read_start_in_chunk, read_end_in_chunk, handle) in
                read_handles
            {
                match handle.await {
                    Ok(Ok(chunk_data)) => {
                        if chunk_data.len() >= read_end_in_chunk {
                            let src = &chunk_data[read_start_in_chunk..read_end_in_chunk];
                            let dst = &mut result[buf_start..buf_start + src.len()];
                            dst.copy_from_slice(src);
                        }
                    }
                    Ok(Err(e)) => return Err(e),
                    Err(e) => {
                        error!("read task panicked for chunk {}: {}", chunk_idx, e);
                        return Err(Errno::EIO);
                    }
                }
            }

            Ok(result)
        })
    }

    /// Write a single user write request through to the relevant OST chunks.
    ///
    /// This makes newly written bytes visible before close()/flush(), which
    /// is critical for toolchains that reopen, mmap, or back-patch their
    /// outputs while the same file handle is still alive.
    pub fn write_through(
        &self,
        ino: u64,
        meta: &FileMeta,
        logical_size: u64,
        offset: u64,
        data: &[u8],
    ) -> Result<(), Errno> {
        if data.is_empty() {
            return Ok(());
        }

        let layout = meta.layout.as_ref().ok_or(Errno::EIO)?;
        let config = self.snap_config();
        let chunk_size = layout.stripe_size;
        let write_end = offset + data.len() as u64;
        let first_chunk = (offset / chunk_size) as u32;
        let last_chunk = ((write_end - 1) / chunk_size) as u32;
        let layout_clone = layout.clone();
        let config_clone = config.clone();

        self.rt.block_on(async move {
            let mut write_handles = Vec::new();

            for chunk_idx in first_chunk..=last_chunk {
                let chunk_file_start = chunk_idx as u64 * chunk_size;
                let chunk_file_end = chunk_file_start + chunk_size;
                let chunk_logical_end = std::cmp::min(logical_size, chunk_file_end);
                let logical_len = chunk_logical_end.saturating_sub(chunk_file_start) as usize;

                let write_start = std::cmp::max(offset, chunk_file_start);
                let write_end_clamped = std::cmp::min(write_end, chunk_file_end);
                if write_start >= write_end_clamped {
                    continue;
                }

                let object_id = StripeLayout::object_id(ino, chunk_idx);
                let ost_index = layout_clone.ost_for_chunk(chunk_idx);
                let addr = ost_addr(&config_clone, ost_index).map_err(rustre_err_to_errno)?;

                // Check if we're writing the entire chunk
                let writes_entire_chunk =
                    write_start == chunk_file_start && write_end_clamped == chunk_file_end;

                if writes_entire_chunk {
                    let chunk_data = data
                        [(write_start - offset) as usize..(write_end_clamped - offset) as usize]
                        .to_vec();
                    let addr_clone = addr.clone();
                    write_handles.push(tokio::spawn(async move {
                        write_chunk_to_ost(&addr_clone, &object_id, &chunk_data).await
                    }));
                } else {
                    // Partial write: read-merge-write
                    let existing = read_chunk_from_ost(&addr, &object_id).await?;
                    let target_len = std::cmp::max(existing.len(), logical_len);
                    let mut chunk_data = vec![0u8; target_len];
                    chunk_data[..existing.len()].copy_from_slice(&existing);

                    let dst_start = (write_start - chunk_file_start) as usize;
                    let dst_end = (write_end_clamped - chunk_file_start) as usize;
                    let src_start = (write_start - offset) as usize;
                    let src_end = (write_end_clamped - offset) as usize;
                    chunk_data[dst_start..dst_end].copy_from_slice(&data[src_start..src_end]);

                    let addr_clone = addr.clone();
                    write_handles.push(tokio::spawn(async move {
                        write_chunk_to_ost(&addr_clone, &object_id, &chunk_data).await
                    }));
                }
            }

            // Wait for all writes to complete
            for handle in write_handles {
                handle.await.map_err(|_| Errno::EIO)??;
            }

            Ok(())
        })
    }

    /// Flush write buffers for an open file.
    ///
    /// Drains the per-fh write buffer, merges overlapping writes into chunk-
    /// aligned blocks, writes them to OST, then syncs the file size to MDS.
    ///
    /// For pending files: uses CommitCreate to set size + make visible.
    /// For committed files: uses SetSizeByIno (with path-based fallback).
    pub fn flush_writes(&self, fh: u64) -> Result<(), Errno> {
        // Drain the write buffer under the per-fh lock
        let (ino, meta, write_buf, writable) = {
            let file_ref = self.open_files.get(&fh).ok_or(Errno::EBADF)?;
            let file = file_ref.value();
            let buf = {
                let mut wb = file.write_buf.lock();
                std::mem::take(&mut *wb)
            };
            (file.ino, file.meta.clone(), buf, file.writable)
        };

        debug!(
            "flush_writes: fh={fh} ino={ino:#x} buf_len={} writable={writable}",
            write_buf.len()
        );

        // Read-only handles: nothing to flush, never update MDS
        if !writable {
            return Ok(());
        }

        // Get the per-inode state
        let state = match self.inode_state.get(&ino) {
            Some(s) => s.value().clone(),
            None => {
                debug!("flush_writes: fh={fh} no inode state, returning");
                return Ok(());
            }
        };

        let current_tracked = state.tracked_size.load(Ordering::Relaxed);

        if write_buf.is_empty() {
            // No buffered writes — just sync size if dirty
            let size_dirty = *state.size_dirty.lock();
            if size_dirty {
                self.sync_size_to_mds(ino, &meta.path, current_tracked)?;
                *state.size_dirty.lock() = false;
                self.inodes.update_size(ino, current_tracked);
            }
            return Ok(());
        }

        let layout = meta.layout.as_ref().ok_or(Errno::EIO)?;
        let config = self.snap_config();
        let chunk_size = layout.stripe_size as usize;

        // Compute the byte range covered by the write buffer
        let mut min_off = u64::MAX;
        let mut max_end: u64 = 0;
        for (off, data) in &write_buf {
            if *off < min_off {
                min_off = *off;
            }
            let end = *off + data.len() as u64;
            if end > max_end {
                max_end = end;
            }
        }

        let max_offset = std::cmp::max(current_tracked, max_end);

        // Build a flat buffer spanning [min_off..max_end), zero-initialized
        let buf_len = (max_end - min_off) as usize;
        let mut flat_buf = vec![0u8; buf_len];
        for (off, data) in &write_buf {
            let start = (*off - min_off) as usize;
            flat_buf[start..start + data.len()].copy_from_slice(data);
        }

        // Determine which chunks are touched
        let first_chunk = (min_off / chunk_size as u64) as u32;
        let last_chunk = if max_end == 0 {
            0
        } else {
            ((max_end - 1) / chunk_size as u64) as u32
        };

        let layout_clone = layout.clone();
        let config_clone = config.clone();

        // Write each affected chunk to the appropriate OST
        self.rt.block_on(async {
            let mut write_handles = Vec::new();

            for chunk_idx in first_chunk..=last_chunk {
                let chunk_file_start = chunk_idx as u64 * chunk_size as u64;
                let chunk_file_end = chunk_file_start + chunk_size as u64;

                let actual_chunk_end = std::cmp::min(chunk_file_end, max_offset);
                let actual_chunk_size = (actual_chunk_end - chunk_file_start) as usize;

                let mut chunk_data = vec![0u8; actual_chunk_size];

                // Copy written bytes into this chunk
                let write_start_in_file = std::cmp::max(chunk_file_start, min_off);
                let write_end_in_file = std::cmp::min(chunk_file_end, max_end);

                if write_start_in_file < write_end_in_file {
                    let dst_start = (write_start_in_file - chunk_file_start) as usize;
                    let dst_end = (write_end_in_file - chunk_file_start) as usize;
                    let src_start = (write_start_in_file - min_off) as usize;
                    let src_end = (write_end_in_file - min_off) as usize;
                    chunk_data[dst_start..dst_end].copy_from_slice(&flat_buf[src_start..src_end]);
                }

                // Read-merge with existing data for partial chunks
                let chunk_fully_covered =
                    min_off <= chunk_file_start && max_end >= actual_chunk_end;
                if !chunk_fully_covered && meta.size > chunk_file_start {
                    let object_id = StripeLayout::object_id(ino, chunk_idx);
                    let ost_index = layout_clone.ost_for_chunk(chunk_idx);
                    let addr = ost_addr(&config_clone, ost_index).map_err(rustre_err_to_errno)?;
                    if let Ok(existing) = read_chunk_from_ost(&addr, &object_id).await {
                        if !existing.is_empty() {
                            for i in 0..std::cmp::min(existing.len(), actual_chunk_size) {
                                let file_off = chunk_file_start + i as u64;
                                if file_off < min_off || file_off >= max_end {
                                    chunk_data[i] = existing[i];
                                }
                            }
                        }
                    }
                }

                let object_id = StripeLayout::object_id(ino, chunk_idx);
                let ost_index = layout_clone.ost_for_chunk(chunk_idx);
                let addr = ost_addr(&config_clone, ost_index).map_err(rustre_err_to_errno)?;

                write_handles.push(tokio::spawn(async move {
                    write_chunk_to_ost(&addr, &object_id, &chunk_data).await
                }));
            }

            for handle in write_handles {
                handle
                    .await
                    .map_err(|_| Errno::EIO)?
                    .map_err(|_| Errno::EIO)?;
            }

            debug!("flush_writes: fh={fh} ino={ino:#x} OST writes complete");
            Ok::<(), Errno>(())
        })?;

        // Sync size to MDS
        debug!("flush_writes: fh={fh} ino={ino:#x} syncing size={max_offset} to MDS");
        self.sync_size_to_mds(ino, &meta.path, max_offset)?;
        debug!("flush_writes: fh={fh} ino={ino:#x} MDS sync complete");

        // Update cached state
        state.tracked_size.store(max_offset, Ordering::Relaxed);
        *state.size_dirty.lock() = false;
        self.inodes.update_size(ino, max_offset);

        Ok(())
    }

    /// Flush all open file handles for a given inode.
    ///
    /// Used by read() and fsync() to ensure read-after-write consistency
    /// across all handles for the same inode.
    ///
    /// IMPORTANT: We collect fh IDs first without locking write_buf to avoid
    /// DashMap shard deadlocks (iter() holds shard locks; locking write_buf
    /// or calling flush_writes while iterating can deadlock).
    pub fn flush_all_handles_for_ino(&self, ino: u64) {
        // Phase 1: collect all fh's for this ino — no write_buf locking!
        let fhs: Vec<u64> = self
            .open_files
            .iter()
            .filter_map(|entry| {
                if entry.value().ino == ino {
                    Some(*entry.key())
                } else {
                    None
                }
            })
            .collect();

        // Phase 2: flush each fh outside the DashMap iterator
        for fh in fhs {
            let _ = self.flush_writes(fh);
        }
    }
}
