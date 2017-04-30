#!/bin/bash

# Step 0, start example/server group with 3 nodes
#     ./output/bin/floyd_node --servers 127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102 --local_ip 127.0.0.1 --local_port 9100 --sdk_port 8900 --data_path ./node1/data --log_path ./node1/log
#     ./output/bin/floyd_node --servers 127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102 --local_ip 127.0.0.1 --local_port 9101 --sdk_port 8901 --data_path ./node2/data --log_path ./node2/log
#     ./output/bin/floyd_node --servers 127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102 --local_ip 127.0.0.1 --local_port 9102 --sdk_port 8902 --data_path ./node3/data --log_path ./node3/log

## 1. Normal case Write Test:
log_file=./0.log
size=30
client=../example/sdk/floyd_client

./$client --server 127.0.0.1:8900 --cmd status --begin 0 --end 1 2> $log_file

./$client --server 127.0.0.1:8902 --cmd write --begin 0 --end $size 2>> $log_file 

./$client --server 127.0.0.1:8900 --cmd dirtyread --begin 0 --end $size 2> tmp0 
./$client --server 127.0.0.1:8901 --cmd dirtyread --begin 0 --end $size 2> tmp1 
./$client --server 127.0.0.1:8902 --cmd dirtyread --begin 0 --end $size 2> tmp2 

diff tmp0 tmp1
if [ $? -ne 0 ]; then
  echo "different value after DirtyRead in tmp0 tmp1"
fi

diff tmp0 tmp2
if [ $? -ne 0 ]; then
  echo "different value after DirtyRead in tmp0 tmp2"
fi

## 2. Normal case Delete Test:
./$client --server 127.0.0.1:8902 --cmd delete --begin 0 --end $size 2>> $log_file 
./$client --server 127.0.0.1:8900 --cmd dirtyread --begin 0 --end $size 2> del.tmp
