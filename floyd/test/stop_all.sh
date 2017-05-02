#!/bin/bash
server=../example/server/output/bin/floyd_node
client=../example/sdk/floyd_client

# Start example/server group with 3 nodes
ps aux|grep "floyd_node" | awk '{print $2}' | xargs kill -9
rm -rf *out *tmp node* *log
