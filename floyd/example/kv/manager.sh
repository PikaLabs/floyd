#!/bin/bash

if [ $1 = "1" ] ; then
  ./output/bin/floyd_node --servers 127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102 --local_ip 127.0.0.1 --local_port 9100 --sdk_port 8900 --data_path ./node1/data --log_path ./node1/log > 1.out 2>&1
elif [ $1 = "2" ] ; then
  ./output/bin/floyd_node --servers 127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102 --local_ip 127.0.0.1 --local_port 9101 --sdk_port 8901 --data_path ./node2/data --log_path ./node2/log > 2.out 2>&1
elif [ $1 = "3" ] ; then
  ./output/bin/floyd_node --servers 127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102 --local_ip 127.0.0.1 --local_port 9102 --sdk_port 8902 --data_path ./node3/data --log_path ./node3/log > 3.out 2>&1
elif [ $1 = "clean" ] ; then
  rm -rf node* *out
fi
