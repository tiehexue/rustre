#!/bin/bash
# Quick-start script: launches MGS + MDS + 2 OSS instances for local testing.
# Press Ctrl-C to stop all processes.

set -e

cleanup() { kill 0 2>/dev/null; }
trap cleanup EXIT INT TERM

cargo build --release --features fuse

rm -rf /tmp/rustre
rm -rf logs
rm -rf /Users/wy/r && mkdir -p /Users/wy/r
fdbcli --exec "writemode on; clearrange \"\" \xff; quit"

./target/release/rustre mgs --log-level debug > /dev/null &
sleep 1
./target/release/rustre mds --log-level debug > /dev/null &
./target/release/rustre oss -i 0 -l 0.0.0.0:9500 --data-dir /tmp/rustre/oss0 --log-level debug > /dev/null &
./target/release/rustre oss -i 1 -l 0.0.0.0:9501 --data-dir /tmp/rustre/oss1 --log-level debug > /dev/null &
sleep 1

umount /Users/wy/r > /dev/null 2>&1 || true
./target/release/rustre --log-level debug mount /Users/wy/r > /dev/null &
sleep 1

cd /Users/wy/r
cp -r /Users/wy/code/r2 rustre
cd rustre
cargo build || true
echo sleeping
sleep 1000000 

wait
