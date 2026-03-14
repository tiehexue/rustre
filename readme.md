# Rustre — Parallel Distributed File System

## What Was Built

A complete parallel, distributed file system inspired by Lustre, implemented in Rust as a single Cargo binary (`rustre`, 3.2MB optimized). The system implements Lustre's core architecture: separated metadata and data paths, RAID-0 striping across multiple object storage targets, and parallel I/O.

## Architecture (Mirroring Lustre)

```
┌─────────┐     ┌─────────┐     ┌─────────┐     ┌─────────┐
│  Client  │────▶│   MGS   │     │   MDS   │     │  OSS×N  │
│ (CLI)    │     │ (config)│     │ (meta)  │     │ (data)  │
└────┬─────┘     └─────────┘     └────┬────┘     └────┬────┘
     │                                │               │
     │  1. Get cluster config ────────┼───────────────│
     │  2. Create file (metadata) ────┘               │
     │  3. Stripe data in parallel ───────────────────┘
```

| Component | Lustre Equivalent | What It Does |
|-----------|------------------|--------------|
| `rustre mgs` | MGS | Cluster config store; registers MDS + OST nodes |
| `rustre mds` | MDS/MDT | Namespace ops, inode allocation, stripe layout computation |
| `rustre oss` | OSS/OST | Object data storage with per-object locking |
| `rustre client` | Client (LLite+LOV+OSC+MDC) | Parallel striped reads/writes, metadata ops |

## Source Files (1,879 lines total)

| File | Lines | Purpose |
|------|-------|---------|
| `src/main.rs` | 116 | CLI entry point with clap subcommands |
| `src/common.rs` | 202 | Shared types: RPC protocol, FileMeta, StripeLayout, errors |
| `src/net.rs` | 75 | Length-prefixed bincode RPC over TCP (≈ PTLRPC/LNet) |
| `src/storage.rs` | 174 | Local object store + metadata persistence with per-object locks |
| `src/mgs.rs` | 151 | Management Server — config storage & distribution |
| `src/mds.rs` | 505 | Metadata Server — namespace, inodes, stripe layout |
| `src/oss.rs` | 117 | Object Storage Server — data objects, usage reporting |
| `src/client.rs` | 539 | Client — parallel striped I/O, file operations |

## Key Features

- **RAID-0 file striping**: Files are split into configurable chunks (default 1MiB) distributed round-robin across OSTs
- **Parallel I/O**: Write and read chunks concurrently using tokio tasks
- **Separated metadata/data paths**: Just like Lustre, metadata ops go to MDS while data goes directly to OSTs
- **Per-object locking**: Prevents race conditions during concurrent writes to the same stripe object
- **Persistent state**: Both metadata (JSON) and object data survive restarts
- **Cluster auto-discovery**: Clients only need the MGS address; everything else is discovered

## Integration Test Results

Successfully tested with a running cluster (1 MGS + 1 MDS + 2 OSTs):

- ✅ Cluster status display
- ✅ Directory creation (`mkdir`)
- ✅ File upload with striping (`put`)
- ✅ Directory listing (`ls`) with stripe info
- ✅ File stat with full stripe layout
- ✅ File download with parallel reassembly (`get`)
- ✅ Small file round-trip: data integrity verified (diff match)

## How to Use

```bash
# Build
cargo build --release

# Start MGS (terminal 1)
rustre mgs

# Start MDS (terminal 2)
rustre mds

# Start OSS instances (terminals 3, 4)
rustre oss -i 0 -l 0.0.0.0:9402
rustre oss -i 1 -l 0.0.0.0:9403

# Client operations
rustre status                           # show cluster
rustre client mkdir /data               # create directory
rustre client put file.dat /data/file.dat -c 2  # striped upload
rustre client ls /data                  # list files
rustre client stat /data/file.dat       # show stripe layout
rustre client get /data/file.dat out.dat  # parallel download
rustre client rm /data/file.dat         # remove
```
