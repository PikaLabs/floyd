# Floyd [中文](https://github.com/Qihoo360/floyd/blob/master/README_CN.md)

[![Build Status](https://travis-ci.org/Qihoo360/floyd.svg?branch=master)](https://travis-ci.org/Qihoo360/floyd)

Floyd is an C++ library based on Raft consensus protocol. 

* [Raft](https://raft.github.io/) is a consensus algorithm  which is easy to understand;
* Floyd is a **library** that could be easily embeded into users' application; 
* Floyd support consistency between cluster nodes by APIs like Read/Write/Delete; 
* Also some query and debug managment APIs： GetLeader/GetServerStatus/set_log_level
* Floyd support lock operation upon raft consensus protocol

## Users

* Floyd has provided high available store for Meta cluster of [Zeppelin](https://github.com/Qihoo360/zeppelin) , which is a huge distributed key-value storage.
* Floyd lock interface has used in our production pika_hub
* The list will goes on.

## Why do we prefer a library to a service?

When we want to coordinate services, ZooKeeper is always a good choice. 
* But we have to maintain another service.
* We must use its' SDK at the same time. 

In our opion, a single service is much more simple than two services. As a result, an embeded library could be a better choice.   


## Floyd's Features and APIs

* APIs and [usage](https://github.com/Qihoo360/floyd/wiki/API%E4%BB%8B%E7%BB%8D%E4%B8%8E%E4%BD%BF%E7%94%A8)

| type      | API             | Status  |
| --------- | --------------- | ------- |
| Consensus | Read            | support |
| Consensus | Write           | support |
| Consensus | Delete          | support |
| Local     | DirtyRead       | support |
| Local     | DirtyWrite      | support |
| Query     | GetLeader       | support |
| Query     | GetServerStatus | support |
| Debug     | set_log_level   | support |

* Raft fetaures

| Language | Leader election + Log Replication | Membership Changes | Log Compaction |
| -------- | --------------------------------- | ------------------ | -------------- |
| C++      | Yes                               | No                 | No             |


## Compile and Have a Try

* Dependencies
    - gcc version 4.8+ to support C++11.
    - protobuf-devel
    - snappy-devel
    - bzip2-devel
    - zlib-devel
    - bzip2
    - submodules:
        - [Pink](https://github.com/Qihoo360/pink)
        - [Slash](https://github.com/Qihoo360/slash)


* Get source code and submodules recursively.
```powershell
git clone --recursive https://github.com/Qihoo360/floyd.git
```
* Compile and get the libfloyd.a library
```
make
```

### Example

Then right now there is three examples in the example directory, go and compile in the corresponding directory

####  example/simple/

contains many cases wrapped floyd into small example

Get all simple example will make command, then every example will start floyd with five node

```
make
```

1. t is a single thread wirte tool to get performance
2. t1 is multi thread program to get performance, in this case, all the writes is from the leader node
3. t2 is an example test node join and leave
4. t4 is an example used to see the message passing by each node in a stable situation
5. t5 used to test single mode floyd, including starting a node and writing data
6. t6 is the same as t1 except that all the writes is from the follower node
7. t7 test write 3 node and then join the other 2 node case

#### example/redis/

raftis is a consensus server with 5 nodes and supporting redis protocol(get/set command). raftis is an example of building a consensus cluster with floyd(floyd is a simple implementation of raft protocol). It's very simple and intuitive. we can test raftis with redis-cli, benchmark with redis redis-benchmark tools. 

compile raftis with command make and then start with run.sh

```
make && sh run.sh
```

```
#!/bin/sh
# start with five node
./output/bin/raftis "127.0.0.1:8901,127.0.0.1:8902,127.0.0.1:8903,127.0.0.1:8904,127.0.0.1:8905" "127.0.0.1" 8901 "./data1/" 6379 &
./output/bin/raftis "127.0.0.1:8901,127.0.0.1:8902,127.0.0.1:8903,127.0.0.1:8904,127.0.0.1:8905" "127.0.0.1" 8902 "./data2/" 6479 &
./output/bin/raftis "127.0.0.1:8901,127.0.0.1:8902,127.0.0.1:8903,127.0.0.1:8904,127.0.0.1:8905" "127.0.0.1" 8903 "./data3/" 6579 &
./output/bin/raftis "127.0.0.1:8901,127.0.0.1:8902,127.0.0.1:8903,127.0.0.1:8904,127.0.0.1:8905" "127.0.0.1" 8904 "./data4/" 6679 &
./output/bin/raftis "127.0.0.1:8901,127.0.0.1:8902,127.0.0.1:8903,127.0.0.1:8904,127.0.0.1:8905" "127.0.0.1" 8905 "./data5/" 6779 &
```


```
└─[$] ./src/redis-benchmark -t set -n 1000000 -r 100000000 -c 20
====== SET ======
  1000000 requests completed in 219.76 seconds
  20 parallel clients
  3 bytes payload
  keep alive: 1

0.00% <= 2 milliseconds
0.00% <= 3 milliseconds
8.72% <= 4 milliseconds
95.39% <= 5 milliseconds
95.96% <= 6 milliseconds
99.21% <= 7 milliseconds
99.66% <= 8 milliseconds
99.97% <= 9 milliseconds
99.97% <= 11 milliseconds
99.97% <= 12 milliseconds
99.97% <= 14 milliseconds
99.97% <= 15 milliseconds
99.99% <= 16 milliseconds
99.99% <= 17 milliseconds
99.99% <= 18 milliseconds
99.99% <= 19 milliseconds
99.99% <= 26 milliseconds
99.99% <= 27 milliseconds
100.00% <= 28 milliseconds
100.00% <= 29 milliseconds
100.00% <= 30 milliseconds
100.00% <= 61 milliseconds
100.00% <= 62 milliseconds
100.00% <= 63 milliseconds
100.00% <= 63 milliseconds
4550.31 requests per second
```
#### example/kv/

A simple consensus kv example contain server and client builded with floyd

## Test
floyd has pass the jepsen test, you can get the test case here
[jepsen](https://github.com/gaodq/jepsen)

## Documents
* [Wikis](https://github.com/Qihoo360/floyd/wiki)

## Contant us

Anyone who is interested in raft protocol, used floyd in your production or has wrote some article about souce code of floyd please contact me, we have a article list.

* email: g-infra-bada@360.cn
* QQ Group: 294254078
* WeChat public: 360基础架构组 
