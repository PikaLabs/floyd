#!/bin/bash

# Step 0, start example/server group with 3 nodes if needed
#sh start_all.sh

## 1. Leader Crash
log_file=./0.log
size=30
client=../example/sdk/floyd_client
server=../example/server/output/bin/floyd_node

echo "[Test 1] Leader Crash\n"

ps aux|grep "node1" | awk '{print $2}' | xargs kill -9
sleep 1
./$client --server 127.0.0.1:8902 --cmd write --begin 0 --end $size

./$client --server 127.0.0.1:8901 --cmd dirtyread --begin 0 --end $size 2> tmp1 
./$client --server 127.0.0.1:8902 --cmd dirtyread --begin 0 --end $size 2> tmp2 

diff tmp1 tmp2
if [ $? -ne 0 ]; then
  echo "1.0 different value after DirtyRead in tmp1 tmp2"
fi

## 2. Restart Node1
nohup ./$server --servers 127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102 --local_ip 127.0.0.1 --local_port 9100 --sdk_port 8900 --data_path ./node1/data --log_path ./node1/log > node1.out 2>&1 &
sleep 1
end=$(( $size + $size ))
./$client --server 127.0.0.1:8900 --cmd write --begin $size --end $end 2>> $log_file 

./$client --server 127.0.0.1:8900 --cmd dirtyread --begin $size --end $end 2> tmp0 
./$client --server 127.0.0.1:8901 --cmd dirtyread --begin $size --end $end 2> tmp1 
./$client --server 127.0.0.1:8902 --cmd dirtyread --begin $size --end $end 2> tmp2 

diff tmp0 tmp1
if [ $? -ne 0 ]; then
  echo "1.2 different value after DirtyRead in tmp0 tmp1"
fi

diff tmp0 tmp2
if [ $? -ne 0 ]; then
  echo "1.3 different value after DirtyRead in tmp0 tmp2"
fi
