# Floyd 中文

Floyd是一个C++实现的Raft一致性协议库。 

* [Raft](https://raft.github.io/)是一个相对于Paxos更容易理解和工程实现的一致性算法;
* Floyd是一个一致性**库**，可以很容易引入到各种项目; 
* Floyd支持集群节点之间的一致性操作，例如：Read/Write/Delete; 
* 同时也支持非一致性的本地数据访问接口: DirtyRead/DirtyWrite;
* 以及查询、管理接口：GetLeader/GetServerStatus/set_log_level

## 用户

* Floyd 目前应用在[Zeppelin](https://github.com/Qihoo360/zeppelin)中，为其Meta集群提供一致性的存储；Zeppeli是一个大容量的分布式key-value存储；
* 陆续会有新的项目在使用；

## 我们为什么倾向于库，而不是一个服务?

当我们在有服务发现、协调和管理的需求时，ZooKeeper是一个很好的选择，但是有一定成本. 
* 我们必须维护一套新的ZooKeeper的服务；
* 我们也必须使用其SDK，去和ZooKeeper服务交互； 

我们认为，一个集成、单一的服务通常会比多个服务更加可控、简单. 所以，作为一个库来使用，能够简化整体的架构.  


## Floyd功能和API

* API和[具体使用](https://github.com/Qihoo360/floyd/wiki/API%E4%BB%8B%E7%BB%8D%E4%B8%8E%E4%BD%BF%E7%94%A8)

| type | API | Status |
| -- | -- | -- |
| 一致性接口 | Read | 支持 |
| 一致性接口 | Write | 支持 |
| 一致性接口 | Delete | 支持 | 
| 一致性接口| Lock | 开发中 | 
| 一致性接口 | UnLock | 开发中 | 
| 一致性接口| GetLease | 开发中 | 
| 本地接口 | DirtyRead | 支持 |
| 本地接口 | DirtyWrite | 支持 |
| 查询 | GetLeader | 支持 |
| 查询 | GetServerStatus | 支持 |
| 调试 | set_log_level | 支持 |

* Raft的功能特性

| Language | Leader election + Log Replication | Membership Changes | Log Compaction |
| -- | -- | -- | -- |
| C++ | Yes | No | No |


## 编译运行

* 依赖
    - gcc 版本4.8+，以支持C++11.
    - protobuf-devel
    - snappy-devel  
    - bzip2-devel
    - zlib-devel
    - bzip2
    - submodules:
        - [nemo-rocksdb](https://github.com/Qihoo360/nemo-rocksdb)
        - [Pink](https://github.com/Qihoo360/pink)
        - [Slash](https://github.com/Qihoo360/slash)


* 1) 拉取代码及子模块依赖.
```powershell
git clone --recursive https://github.com/Qihoo360/floyd.git
```
* 2) 编译Floyd库
```powershell
make
```
* 3) 编译example
`make test`
* 4) 运行example/server集群

```powershell
cd exampler/server
./output/bin/floyd_node --servers 127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102 --local_ip 127.0.0.1 --local_port 9100 --sdk_port 8900 --data_path ./node1/data --log_path ./node1/log
./output/bin/floyd_node --servers 127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102 --local_ip 127.0.0.1 --local_port 9101 --sdk_port 8901 --data_path ./node2/data --log_path ./node2/log
./output/bin/floyd_node --servers 127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102 --local_ip 127.0.0.1 --local_port 9102 --sdk_port 8902 --data_path ./node3/data --log_path ./node3/log
```

* 5) 运行client

```powershell
# ./floyd_client
Usage:
  ./client --server ip:port
           --cmd [read | write | delete | status | debug_on | debug_off]
           --begin id0 --end id1
# ./floyd_client --server 127.0.0.1:8901 --cmd status --begin 0 --end 1
```

## 文档
* [Wikis](https://github.com/Qihoo360/floyd/wiki)

## 联系我们

* email: g-infra-bada@360.cn
* QQ Group: 294254078
* WeChat public: 360基础架构组 
