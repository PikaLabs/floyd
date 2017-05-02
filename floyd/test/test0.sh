#!/bin/bash

# Step 0, start example/server group with 3 nodes
sh start_all.sh

## 1. Normal case Write Test:
log_file=./0.log
size=30
client=../example/sdk/floyd_client

./$client --server 127.0.0.1:8900 --cmd status --begin 0 --end 1 2> $log_file

######################
### Write a follower
######################
./$client --server 127.0.0.1:8902 --cmd write --begin 0 --end $size 2>> $log_file 

./$client --server 127.0.0.1:8900 --cmd dirtyread --begin 0 --end $size 2> tmp0 
./$client --server 127.0.0.1:8901 --cmd dirtyread --begin 0 --end $size 2> tmp1 
./$client --server 127.0.0.1:8902 --cmd dirtyread --begin 0 --end $size 2> tmp2 

echo "[Test 0] Normal Write/Delete\n"
diff tmp0 tmp1
if [ $? -ne 0 ]; then
  echo "0.0 different value after DirtyRead in tmp0 tmp1"
fi

diff tmp0 tmp2
if [ $? -ne 0 ]; then
  echo "0.1 different value after DirtyRead in tmp0 tmp2"
fi

######################
### Write leader
######################
end=$(( $size + $size ))
./$client --server 127.0.0.1:8900 --cmd write --begin $size --end $end 2>> $log_file 

./$client --server 127.0.0.1:8900 --cmd dirtyread --begin $size --end $end 2> tmp0 
./$client --server 127.0.0.1:8901 --cmd dirtyread --begin $size --end $end 2> tmp1 
./$client --server 127.0.0.1:8902 --cmd dirtyread --begin $size --end $end 2> tmp2 

diff tmp0 tmp1
if [ $? -ne 0 ]; then
  echo "0.2 different value after DirtyRead in tmp0 tmp1"
fi

diff tmp0 tmp2
if [ $? -ne 0 ]; then
  echo "0.3 different value after DirtyRead in tmp0 tmp2"
fi


## 2. Normal case Delete Test:
./$client --server 127.0.0.1:8902 --cmd delete --begin 0 --end $size 2>> $log_file 
./$client --server 127.0.0.1:8900 --cmd dirtyread --begin 0 --end $size 2> del.tmp
