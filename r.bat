start rustre mgs -l 172.24.8.132:9400
sleep 2
start rustre mds -m 172.24.8.132:9400 -l 172.24.8.132:9401
start rustre oss -i 0 -m 172.24.8.132:9400 -l 172.24.8.132:9500 --data-dir /tmp/rustre/oss0
start rustre oss -i 1 -m 172.24.8.132:9400 -l 172.24.8.132:9501 --data-dir /tmp/rustre/oss1