#!/bin/bash
# Quick-start script: launches MGS + MDS + 4 OSS instances for local testing.
# Press Ctrl-C to stop all processes.

set -e

cleanup() { kill 0 2>/dev/null; }
trap cleanup EXIT INT TERM

cargo run -- mgs --log-level debug &
sleep 1
cargo run -- mds --log-level debug &
cargo run -- oss -i 0 -l 0.0.0.0:9500 --data-dir /tmp/rustre/oss0 --log-level debug &
cargo run -- oss -i 1 -l 0.0.0.0:9501 --data-dir /tmp/rustre/oss1 --log-level debug &
cargo run -- oss -i 2 -l 0.0.0.0:9502 --data-dir /tmp/rustre/oss2 --log-level debug &
cargo run -- oss -i 3 -l 0.0.0.0:9503 --data-dir /tmp/rustre/oss3 --log-level debug &

wait
