start cargo run -- mgs
start cargo run -- mds
start cargo run -- oss -i 0 -l 0.0.0.0:9500 --data-dir /tmp/rustre/oss0 --log-level debug
start cargo run -- oss -i 1 -l 0.0.0.0:9501 --data-dir /tmp/rustre/oss1 --log-level debug
start cargo run -- oss -i 2 -l 0.0.0.0:9502 --data-dir /tmp/rustre/oss2 --log-level debug
start cargo run -- oss -i 3 -l 0.0.0.0:9503 --data-dir /tmp/rustre/oss3 --log-level debug
