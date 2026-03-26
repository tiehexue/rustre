#!/bin/bash
# Quick-start script: launches MGS + MDS + 2 OSS instances for local testing.
# Press Ctrl-C to stop all processes.

set -e

cleanup() { kill 0 2>/dev/null; }
trap cleanup EXIT INT TERM

./target/release/rustre mgs --log-level debug &
sleep 1
./target/release/rustre mds --log-level debug &
./target/release/rustre oss -i 0 -l 0.0.0.0:9500 --data-dir /tmp/rustre/oss0 --log-level debug &
./target/release/rustre oss -i 1 -l 0.0.0.0:9501 --data-dir /tmp/rustre/oss1 --log-level debug &

wait
