# Floyd [中文](https://github.com/Qihoo360/floyd/blob/master/README_CN.md)

Floyd is an C++ library based on Raft consensus protocol. 

* [Raft](https://raft.github.io/) is a consensus algorithm  which is easy to understand;
* Floyd is an **library** that could be easily embeded into users' application; 
* Floyd support consistency between cluster nodes by APIs like Read/Write/Delete; 
* Also we support direct access to data without consistency: DirtyRead/DirtyWrite;
* Also some query and debug managment APIs： GetLeader/GetServerStatus/set_log_level

## Users

* Floyd has provided high available store for Meta cluster of [Zeppelin](https://github.com/Qihoo360/zeppelin) , which is a huge distributed key-value storage.
* The list will goes on.

## Why do we prefer a library to a service?

When we want to coordinate services, ZooKeeper is always a good choice. 
* But we have to maintain another service.
* We must use its' SDK at the same time. 

In our opion, a single service is much more simple than two services. As a result, an embeded library could be a better choice.   


## Floyd's Features and APIs

* APIs and [usage](https://github.com/Qihoo360/floyd/wiki/API%E4%BB%8B%E7%BB%8D%E4%B8%8E%E4%BD%BF%E7%94%A8)

| type | API | Status |
| -- | -- | -- |
| Consensus | Read | support |
| Consensus | Write | support |
| Consensus | Delete | support | 
| Local | DirtyRead | support |
| Local | DirtyWrite | support |
| Query | GetLeader | support |
| Query | GetServerStatus | support |
| Debug | set_log_level | support |

* Raft fetaures

| Language | Leader election + Log Replication | Membership Changes | Log Compaction |
| -- | -- | -- | -- |
| C++ | Yes | No | No |


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

Then right now there is three examples in the example directory, go and compile in the corresponding directory

example/simple/  contains many cases wrapped floyd into small example
example/redis/   raftis is a consensus server with 5 nodes and supporting redis protocol(get/set command). raftis is an example of building a consensus cluster with floyd(floyd is a simple implementation of raft protocol). It's very simple and intuitive. we can test raftis with redis-cli, benchmark with redis redis-benchmark tools. 
example/kv/   A simple consensus kv example contain server and client builded with floyd

## Documents
* [Wikis](https://github.com/Qihoo360/floyd/wiki)

## Contant us

* email: g-infra-bada@360.cn
* QQ Group: 294254078
* WeChat public: 360基础架构组 
