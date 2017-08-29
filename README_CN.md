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

| type  | API             | Status |
| ----- | --------------- | ------ |
| 一致性接口 | Read            | 支持     |
| 一致性接口 | Write           | 支持     |
| 一致性接口 | Delete          | 支持     |
| 本地接口  | DirtyRead       | 支持     |
| 本地接口  | DirtyWrite      | 支持     |
| 查询    | GetLeader       | 支持     |
| 查询    | GetServerStatus | 支持     |
| 调试    | set_log_level   | 支持     |

* Raft的功能特性

| Language | Leader election + Log Replication | Membership Changes | Log Compaction |
| -------- | --------------------------------- | ------------------ | -------------- |
| C++      | Yes                               | No                 | No             |


## 编译运行

* 依赖
    - gcc 版本4.8+，以支持C++11.
    - protobuf-devel
    - snappy-devel  
    - bzip2-devel
    - zlib-devel
    - bzip2
    - submodules:
        - [Pink](https://github.com/Qihoo360/pink)
        - [Slash](https://github.com/Qihoo360/slash)


* 1) 拉取代码及子模块依赖.
```powershell
git clone --recursive https://github.com/Qihoo360/floyd.git
```
* 2) 编译Floyd库, 并且获得静态库libfloyd.a
```powershell
make
```

### 示例

目前我们实现了3个使用floyd 的例子, 在example/ 目录下面, 可以直接去对应的目录进行编译使用

####  example/simple/

这个simple 示例主要包含好几个本机就能够运行的使用floyd 的例子.
在这些例子里面, 都会在一个进程启动5个 floyd 对象, 然后会有对象的停止, 重新启动等等操作来观察floyd 的一致性

执行 make 就可以编译所以的示例.在 output/bin/ 目录下面
```
make
```

1. t 是标准的示例, 运行起来以后会启动5个节点, 然后会不断的写入数据, 并统计QPS 以及展示集群目前的状态信息
2. t1 是一个多线程压测的示例, 在这例子里面, write 是直接写入到Leader node的, 在我们的机器上, 可以获得差不多2w 的性能
3. t2 这个例子主要用来示范节点的加入和删除操作是否正常, 可以通过观看每一个节点的LOG 信息来看raft 协议的过程
4. t4 是节点起来以后就不做任何操作的稳定的raft 集群, 通常用来查看raft 协议中传输的消息, 可以通过看每一个节点的LOG 信息来看一个稳定运行的raft 集群的日志信息. t4 也经常用来配合tools 下面的cl 来生成不同的raft log 来验证raft 的log 不一致的情况下, 如何进行log replication
5. t5 是用来示例单节点的floyd 运行以及写入数据
6. t6 是和t1 一样是一个多线程压测的示例, 但是写入是从客户端写入, 因此性能会比t1 要来的差, 在我们的机器上, 差不多可以是5500 的性能
7. t7 同样用来测试节点的加入和退出, 但是会在节点退出以后继续写入数据, 验证3个节点存活, 然后又有2个节点加入的过程

#### example/redis/

raftis 是一个5个节点的floyd server. raftis 支持redis 协议里面的get, set 命令. 
raftis 是一个使用floyd 构建一致性集群的示例, 一般使用floyd 我们都推荐这么用, 非常的简单. raftis 可以直接使用 reids-cli, redis-benchmark 进行压测


使用make 编译 raftis, 然后运行run.sh 可以启动5个节点.  当然这里运行的5个节点都在本地, 你完全可以在5个不同的机器运行这5个节点

```
make
sh run.sh
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

一个使用floyd 实现强一致协议的server, 包含server, client



## 文档
* [Wikis](https://github.com/Qihoo360/floyd/wiki)

## 联系我们

* email: g-infra-bada@360.cn
* QQ Group: 294254078
* WeChat public: 360基础架构组 
