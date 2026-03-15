# Rustre — Parallel Distributed File System

## What Was Built

A complete parallel, distributed file system inspired by Lustre, implemented in Rust as a single Cargo binary (`rustre`). The system implements Lustre's core architecture: separated metadata and data paths, RAID-0 striping across multiple object storage targets, and parallel I/O.

## Architecture (Mirroring Lustre)

```
┌─────────┐     ┌─────────────┐     ┌──────────────┐     ┌───────────┐
│  Client  │────▶│  MGS (×N)   │     │   MDS (×N)   │     │  OSS×N    │
│ (CLI)    │     │ (FDB-backed)│     │ (FDB-backed) │     │ (RocksDB) │
└────┬─────┘     └──────┬──────┘     └──────┬───────┘     └─────┬─────┘
     │                  │                   │                    │
     │  1. Get cluster config ──────┼───────────────────│
     │  2. Create file (metadata) ──┘                    │
     │  3. Stripe data in parallel ─────────────────────┘
     │                  │                   │
     │           ┌──────┴───────────────────┴──────┐
     │           │         FoundationDB            │
     │           │  Shared state (cluster + meta)  │
     │           └─────────────────────────────────┘
```

| Component | Lustre Equivalent | What It Does | Storage Backend |
|-----------|------------------|--------------|-----------------|
| `rustre mgs` | MGS | Cluster config store; registers MDS + OST nodes | **FoundationDB** (distributed, stateless HA) |
| `rustre mds` | MDS/MDT | Namespace ops, inode allocation, stripe layout computation | **FoundationDB** (distributed, stateless HA) |
| `rustre oss` | OSS/OST | Object data storage with per-object operations | **RocksDB** (embedded KV store) |
| `rustre client` | Client (LLite+LOV+OSC+MDC) | Parallel striped reads/writes, metadata ops | — |

## Key Architecture Decisions

### 1. RocksDB Object Store (OSS)

Each OST stores objects in RocksDB instead of flat files:
- **Key**: `obj:{ino_hex}:{stripe_seq_hex}` (e.g., `obj:000000000000000a:00000003`)
- **Value**: raw bytes of the stripe chunk

Benefits over flat files:
- No per-object filesystem overhead (inodes, directory entries)
- Built-in LZ4/Snappy compression
- Atomic writes via WAL
- Efficient prefix-scan for bulk operations (e.g., delete all objects for an inode)
- Much better performance for millions of small objects

### 2. Deterministic Stripe Layout

Object IDs and OST assignments are computed deterministically from `(ino, stripe_seq)`:

```
object_id = format!("{:016x}:{:08x}", ino, stripe_seq)
ost_index = (stripe_offset + stripe_seq) % ost_count
stripe_offset = ino % ost_count  (spreads files' starting OSTs)
```

This means:
- **No per-chunk object list** stored in metadata — everything is computed on-the-fly
- The `StripeLayout` stores only `(stripe_count, stripe_size, stripe_offset)`
- Given a file's inode and size, any node can reconstruct the full stripe map
- **Equal distribution** across OSTs is guaranteed by round-robin assignment

### 3. FoundationDB for MGS (Stateless HA)

Multiple MGS instances can run simultaneously as stateless proxies to FoundationDB:

**Key schema** (under configurable cluster prefix, default `rustre/`):
```
rustre/mds/{address}            → MdsInfo (bincode)
rustre/ost/{ost_index:08x}      → OstInfo (bincode)
rustre/ost_usage/{index:08x}    → u64 used_bytes (bincode)
```

Benefits:
- **Zero single point of failure** — any MGS can serve any request
- FDB provides serializable transactions — no split-brain
- OST usage updates separated from registration to avoid transaction conflicts
- Clients connect to any available MGS

### 4. FoundationDB for MDS (Stateless HA)

All filesystem metadata lives in FoundationDB, making MDS completely stateless. Multiple MDS instances can run concurrently against the same FDB cluster.

**Key schema** (under `{cluster}/mds_meta/`):
```
rustre/mds_meta/ino/{ino:016x}                              → FileMeta (bincode)
rustre/mds_meta/path/{normalized_path}                       → u64 ino (8 bytes LE)
rustre/mds_meta/children/{parent_ino:016x}/{child_ino:016x}  → empty (existence = membership)
rustre/mds_meta/next_ino                                     → u64 (8 bytes LE)
```

Design rationale:
- **Children as individual keys**: avoids read-modify-write contention on popular directories. Adding/removing a child is a single set/clear; listing is a range scan.
- **Inode allocation via FDB transactions**: safe concurrent allocation across multiple MDS instances.
- **Transactional creates/unlinks**: inode + path + children updated atomically in a single FDB transaction — no partial state possible.
- **No local disk state**: MDS needs zero local storage. If one MDS crashes, any other MDS picks up seamlessly.

## Source Files

| File | Purpose |
|------|---------|
| `src/main.rs` | CLI entry point with clap subcommands, FDB network init for MGS + MDS |
| `src/common.rs` | Shared types: RPC protocol, FileMeta, StripeLayout (with deterministic helpers), errors |
| `src/net.rs` | Length-prefixed bincode RPC over TCP (≈ PTLRPC/LNet) |
| `src/storage.rs` | RocksObjectStore (RocksDB), FdbMetaStore (MGS), FdbMdsStore (MDS) — all FoundationDB-backed |
| `src/mgs.rs` | Management Server — FDB-backed config storage & distribution |
| `src/mds.rs` | Metadata Server — FDB-backed namespace, inodes, stripe layout (stateless HA) |
| `src/oss.rs` | Object Storage Server — RocksDB data objects, bulk inode deletion |
| `src/client.rs` | Client — deterministic stripe computation, parallel I/O |

## Key Features

- **RAID-0 file striping**: Files are split into configurable chunks (default 1 MiB) distributed round-robin across OSTs
- **Deterministic object IDs**: Derived from `(ino, stripe_seq)` — no metadata lookup needed for stripe map
- **Parallel I/O**: Write and read chunks concurrently using tokio tasks
- **Separated metadata/data paths**: Just like Lustre, metadata ops go to MDS while data goes directly to OSTs
- **RocksDB object store**: Efficient embedded KV store replaces flat files for object data
- **FoundationDB cluster state**: MGS is stateless and horizontally scalable
- **FoundationDB metadata**: MDS is stateless and horizontally scalable — all inodes, paths, and directory entries in FDB
- **Transactional metadata ops**: Creates, unlinks, and renames are atomic FDB transactions — no partial state
- **Bulk inode deletion**: OSTs can delete all objects for an inode via prefix scan
- **Persistent state**: Metadata (FDB) and object data (RocksDB) survive restarts
- **Cluster auto-discovery**: Clients only need the MGS address; everything else is discovered

## Prerequisites

- **Rust** toolchain (1.85+)
- **FoundationDB** client library (`libfdb_c`) — for MGS and MDS functionality
  - macOS ARM64: Download from [FDB releases](https://github.com/apple/foundationdb/releases), extract the `_arm64.pkg`
  - Linux: Install `foundationdb-clients` package
- **FoundationDB server** — running cluster for MGS + MDS state storage
- RocksDB is built from source automatically via `librocksdb-sys`

## How to Use

```bash
# Build
cargo build --release

# Start FoundationDB (must be running for MGS + MDS)
# See: https://apple.github.io/foundationdb/getting-started-mac.html

# Start MGS — stateless, can run multiple instances (terminal 1)
rustre mgs
# Or with custom cluster name:
rustre mgs --cluster-name my-cluster

# Start MDS — stateless, can run multiple instances (terminal 2)
rustre mds
# Or with custom cluster name (must match MGS):
rustre mds --cluster-name my-cluster

# Start OSS instances with RocksDB storage (terminals 3, 4)
rustre oss -i 0 -l 0.0.0.0:9402 -d /data/ost0
rustre oss -i 1 -l 0.0.0.0:9403 -d /data/ost1

# Client operations
rustre status                           # show cluster
rustre client mkdir /data               # create directory
rustre client put file.dat /data/file.dat -c 2  # striped upload
rustre client ls /data                  # list files
rustre client stat /data/file.dat       # show stripe layout + object mapping
rustre client get /data/file.dat out.dat  # parallel download
rustre client rm /data/file.dat         # remove (bulk delete from all OSTs)

# Run additional stateless instances for HA:
rustre mgs -l 0.0.0.0:9410             # second MGS, same FDB backend
rustre mds -l 0.0.0.0:9411             # second MDS, same FDB backend
```
