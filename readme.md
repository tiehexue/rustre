# Rustre — Parallel Distributed File System

A parallel, distributed file system inspired by [Lustre](https://www.lustre.org/), implemented in Rust as a single binary. Separated metadata and data paths, striping/repicating across object storage targets, zero-copy I/O, and FoundationDB-backed stateless servers.

## Architecture

```
                    ┌──────────────┐
           ┌───────│  MGS (×N)    │───────┐
           │       │  FDB-backed  │       │
           │       └──────────────┘       │
      register                       register
      + heartbeat                    + heartbeat
           │                              │
    ┌──────┴──────┐                ┌──────┴──────┐
    │  MDS (×N)   │                │  OSS (×N)   │
    │  FDB-backed │                │  FileStore  │
    └──────┬──────┘                └──────┬──────┘
           │                              │
           └──────────┬───────────────────┘
                      │
               ┌──────┴──────┐
               │   Client    │
               └─────────────┘
```

| Component | Lustre Equivalent | Role | Storage |
|-----------|------------------|------|---------|
| `rustre mgs` | MGS | Cluster config, node registration, heartbeats | FoundationDB |
| `rustre mds` | MDS / MDT | Namespace, inodes, stripe layout computation | FoundationDB |
| `rustre oss` | OSS / OST | Object data storage, zero-copy reads/writes | Local filesystem |
| `rustre client` | Client (LLite+LOV+OSC+MDC) | Parallel striped I/O, metadata ops | — |

## Key Design Decisions

### Stateless Servers (MGS + MDS)

Both MGS and MDS store all state in FoundationDB. Multiple instances run concurrently with no coordination — they are pure stateless proxies. If one crashes, any other instance picks up seamlessly.

### File-Backed Object Store (OSS)

Each OST stores objects as plain files on the local filesystem:

```
{data_dir}/objects/{ino_prefix}/{ino}_{chunk_index}
```

This enables kernel-level zero-copy via `sendfile()` (macOS/Linux) and `TransmitFile()` (Windows) — data moves directly between disk and network socket without touching userspace buffers.

### Deterministic Stripe Layout

Object IDs and OST assignments are computed from `(ino, stripe_seq)` — no per-chunk object list stored in metadata:

```
object_id  = format!("{:016x}:{:08x}", ino, seq)
ost_index  = ost_indices[seq % stripe_count]
```

Given a file's inode and size, any node can reconstruct the full stripe map.

### FoundationDB Key Schemas

**MGS** (under `{cluster}/`):
```
{cluster}/mds/{address}          → MdsInfo (bincode)
{cluster}/ost/{index:08x}       → OstInfo (bincode)
{cluster}/ost_usage/{index:08x} → u64 used_bytes
```

**MDS** (under `{cluster}/mds_meta/`):
```
ino/{ino:016x}                              → FileMeta (bincode)
path/{normalized_path}                       → u64 ino (8 bytes LE)
children/{parent_ino:016x}/{child_ino:016x} → empty (existence = membership)
next_ino                                     → u64 (atomic counter)
```

Children are individual keys — adding/removing a child is a single `set`/`clear`, listing is a range scan. Creates and unlinks are atomic FDB transactions.

## Source Files

| Module | Files | Purpose |
|--------|-------|---------|
| `main.rs` | — | CLI entry point, FDB network init, subcommand dispatch |
| `client/` | `mod.rs` `commands.rs` `operations.rs` `put.rs` `rm.rs` `status.rs` | Client operations: put, get, ls, stat, mkdir, rm, status |
| `mgs/` | `mod.rs` `server.rs` `config.rs` `heartbeat.rs` | Management Server: config store, node registration, heartbeats |
| `mds/` | `mod.rs` `server.rs` `operations.rs` `path_utils.rs` | Metadata Server: namespace, inodes, stripe layout computation |
| `oss/` | `mod.rs` | Object Storage Server: data I/O, zero-copy transfers |
| `rpc/` | `mod.rs` | Length-prefixed bincode RPC over TCP (≈ PTLRPC + LNet) |
| `storage/` | `mod.rs` `fdb_meta_store.rs` `fdb_mds_store.rs` `file_store.rs` | FDB stores for MGS/MDS, file-backed object store for OSS |
| `types/` | `mod.rs` | Shared types: StripeLayout, FileMeta, OstInfo, ClusterConfig |
| `error/` | `mod.rs` | Error types (thiserror-derived) |
| `utils/` | `mod.rs` `logging.rs` | Dual-output logging (stdout + file) with local timestamps |
| `zerocopy/` | `mod.rs` `transfer.rs` | Platform-specific sendfile/TransmitFile + striped transfer logic |

## Features

- **Striping** — files split into configurable chunks distributed round-robin across OSTs
- **Zero-copy I/O** — `sendfile()` / `TransmitFile()` for both client→OSS writes and OSS→client reads
- **Parallel I/O** — concurrent chunk reads/writes via tokio tasks
- **Separated metadata/data paths** — metadata to MDS, data directly to OSTs
- **Stateless HA** — MGS and MDS are horizontally scalable, all state in FoundationDB
- **Transactional metadata** — creates, unlinks, and renames are atomic FDB transactions
- **Deterministic object IDs** — derived from `(ino, stripe_seq)`, no metadata lookup needed
- **Heartbeat monitoring** — MGS probes MDS/OSS every second, removes nodes after 2 consecutive failures
- **Replication** — configurable replica count per file, replicas written in parallel
- **Cluster auto-discovery** — clients only need the MGS address

## Prerequisites

- **Rust** toolchain (1.85+)
- **FoundationDB** client library (`libfdb_c`)
  - macOS ARM64: [FDB releases](https://github.com/apple/foundationdb/releases) → `_arm64.pkg`
  - Linux: `foundationdb-clients` package
- **FoundationDB server** — running cluster for MGS + MDS state

## Usage

```bash
# Build
cargo build --release

# Start cluster (FoundationDB must be running)
rustre mgs                                              # MGS
rustre mds                                              # MDS
rustre oss -i 0 -l 0.0.0.0:9500 -d /data/ost0          # OSS #0
rustre oss -i 1 -l 0.0.0.0:9501 -d /data/ost1          # OSS #1

# Client operations
rustre status                                           # cluster status
rustre client mkdir /data                               # create directory
rustre client put file.dat /data/file.dat -c 2          # striped upload
rustre client ls /data                                  # list directory
rustre client stat /data/file.dat                       # metadata + stripe map
rustre client get /data/file.dat out.dat                # parallel download
rustre client rm /data/file.dat                         # remove file

# Scale out (stateless — just start more instances)
rustre mgs -l 0.0.0.0:9410                              # additional MGS
rustre mds -l 0.0.0.0:9411                              # additional MDS
```

## Next Steps
- **Zol** replace file store with zvol+chunk allocator, or raw block devices with chunk allocator. Rocksdb on filesystem for metadata in oss.
- **Posix** make it mountable
- **Testing** does this really works?
- **RDMA** no env for testing...
