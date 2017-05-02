#!/bin/bash
server=../example/server/output/bin/floyd_node
client=../example/sdk/floyd_client

# Start example/server group with 3 nodes
nohup ./$server --servers 127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102 --local_ip 127.0.0.1 --local_port 9100 --sdk_port 8900 --data_path ./node1/data --log_path ./node1/log > node1.out 2>&1 &
nohup sleep 3;
nohup ./$server --servers 127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102 --local_ip 127.0.0.1 --local_port 9101 --sdk_port 8901 --data_path ./node2/data --log_path ./node2/log > node2.out 2>&1 &
nohup ./$server --servers 127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102 --local_ip 127.0.0.1 --local_port 9102 --sdk_port 8902 --data_path ./node3/data --log_path ./node3/log > node3.out 2>&1 &

sleep 2;
./$client --server 127.0.0.1:8900 --cmd status --begin 0 --end 1
