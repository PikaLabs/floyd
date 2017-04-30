#!/bin/bash

log_file=0.log
size=30
./floyd_client --server 127.0.0.1:8900 --cmd status --begin 0 --end 1 2> $log_file

./floyd_client --server 127.0.0.1:8902 --cmd write --begin 0 --end $size 2>> $log_file 

./floyd_client --server 127.0.0.1:8900 --cmd dirtyread --begin 0 --end $size 2> tmp0 
./floyd_client --server 127.0.0.1:8901 --cmd dirtyread --begin 0 --end $size 2> tmp1 
./floyd_client --server 127.0.0.1:8902 --cmd dirtyread --begin 0 --end $size 2> tmp2

diff tmp0 tmp1
if [ $? -ne 0 ]; then
  echo "different value in tmp0 tmp1"
fi

diff tmp0 tmp2
if [ $? -ne 0 ]; then
  echo "different value in tmp0 tmp2"
fi

