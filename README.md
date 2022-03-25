# FastOSS 分布式对象存储系统

![](https://images-1257369645.cos.ap-chengdu.myqcloud.com/FastOSS/FastOSS%E6%9E%B6%E6%9E%84.png)



## 系统架构介绍

采用中间控制节点架构，由中间服务器协调管理存储系统。

- **Tracker**：中间控制节点，负责管理对象的元数据和存储桶权限等信息。
- **Storage**：存储节点，存储对象数据。
- **Proxy**：代理服务器，作为整个系统对外的接口。Proxy服务器是无状态的，可以通过Nginx做负载均衡。
- **Zookeeper**：使用Zookeeper来管理Storage服务器，解除Tracker和Storage的耦合，避免Tracker管理大量的Storage服务器连接和心跳。
- **Kafka**：Tracker和Storage之间有大量的消息通信，为了提高消息传递的稳定性和吞吐量，FastOSS使用Kafka作消息队列。
- **Prometheus**：监控服务，FastOSS使用Prometheus来收集和保存存储服务器的运行指标，并在Proxy服务中提供了相关接口方便管理员用户访问。

## Tracker服务

Tracker记录了对象存储系统中每个对象的元数据和物理位置（在哪台Storage服务器上），同时它也作为访问权限的认证中心。

### BitCask存储模型

为了保证不丢失对象的位置信息，Tracker使用了BitCask模型来存储KV数据。BitCask的优势在于，它将数据的写操作（增删改）全部转化成了顺序写，相比于随机写大大提升了写入速度。

![](https://images-1257369645.cos.ap-chengdu.myqcloud.com/FastOSS/bitCask%E5%AD%98%E5%82%A8%E6%A8%A1%E5%9E%8B.png)

### Bitcask的优缺点

Bitcask的实现十分简单，同时它具有较好的单条记录写入和读取性能。不过它也有一些缺点。

- **无序**：记录是按照写入顺序存储的，对于需要排序的查询，Bitcask很难做到。
- **范围查询**：由于无序性，Bitcask无法做到范围查询，如果要查询一个范围内的记录，Bitcask会先从内存中找到这些Key的位置，然后通过多次磁盘IO找到每一条记录，多次的磁盘IO会严重影响查询性能。
- **数据压缩**：由于修改和删除记录都不会导致覆盖旧的记录，这就需要有后台的进程负责清理这些无效数据。就像GC一样，清理的过程会消耗时间。

既然Bitcask由如此多的缺点，那么为什么FastOSS还要使用它？

首先，在对象系统中对元数据的访问都是单个的，Bitcask的无序性和无法范围查询并不会有太大的影响。其次，FastOSS的应用场景决定了，文件的**修改和删除频率低于读取和新增的频率**。在这种**多读多增少改**的场景下，Bitcask是可行的。

### 一致性Hash负载均衡

在选择主副本存储位置时，Tracker服务器目前使用一致性Hash算法来保证Storage负载均衡。在后续的开发中将陆续引入轮询、加权轮询等更多的负载均衡算法。

### 存储桶访问权限

对象存储不具有文件系统的目录层级结构，不过用户可以使用存储桶来集中管理对象。在FastOSS中，存储桶有三种访问权限设置：

- **私有**（Private）：对存储桶的读写全部是私有操作。
- **公共读**（Public_Read）：存储桶开放读请求，写请求为私有。
- **公共写**（Public_Write）：存储桶完全开放读写请求。

每个存储桶在创建后会生成唯一的AppID、AccessKey和SecretKey。用户通过这三个字符串来访问存储桶。

对于私有请求，用户需要通过SDK的Token工具，根据AccessKey和SecretKey生成访问Token，并在请求中携带Token。

### Tracker缺陷

目前的Tracker是系统中唯一的单点，也就是整个系统最大的缺陷。为了避免单点的不可靠性，后续的更新会将Tracker集群化来达到高可用要求。

## Storage服务

Storage存储了对象的数据和元数据。

### 大量小文件存储优化

由于系统的定位是存储大量小文件，所以Storage服务对小文件存储做了优化。

操作系统对每一个文件都会记录一份元数据，也就是Linux中的inode，其中包括权限组、用户组等信息。这些信息在对象存储中没有任何意义，但是会占据磁盘空间。在海量小文件的情境下，大量的文件系统元数据会占用大量磁盘资源。

为了解决该问题，FastOSS将小文件合并成大文件（Chunk），并规定每个大文件大小固定。单个的小文件都会已追加的形式写入大文件。通过这种方式减少额外的元数据存储，提升资源利用率。

删除文件时只需要在内存标记删除，后台会有专门的线程来压缩大文件中被删除的部分。

![](https://images-1257369645.cos.ap-chengdu.myqcloud.com/FastOSS/chunk.png)

### 备份机制

在FastOSS中，一个对象会保存多个副本，副本的位置由Tracker服务器选择和记录。上传时客户端会尝试向每个副本机器传输数据，直到有一台机器成功收到并返回。成功接收的副本就成了该对象的主副本，其他备份机器从主副本读取并保存从副本。

### 数据可靠性

备份是最常见的数据可靠机制，FastOSS采用了最常用的三副本机制。副本复制是最简单的可靠性机制，不过它会导致存储空间的浪费。所以FastOSS在后续更新中会逐渐添加如**EC码**等更高级的机制。

### EditLog编辑日志

FastOSS中的元数据除了保存在内存中还会用编辑日志的方式写入硬盘，这种机制类似Redis的**AOF持久化**。

众所周知，磁盘的写入速度是远不如内存的，为了提高元数据刷盘的效率，FastOSS不会把每一条元数据立即写入磁盘，而是将元数据记录先写入内存的缓冲区，再按照刷盘规则写入磁盘。

## Proxy服务

Proxy类似存储系统的网关，它作为整个系统对外的接口。对系统内的服务器，它屏蔽了请求的来源。对系统外的客户端，它屏蔽了系统内部的服务器地址。这样既保证了系统内部服务器的安全，也对用户隐藏了系统内部的服务分布。

因为Proxy服务是无状态的，所以可以部署多台Proxy服务器并使用Nginx等工具做负载均衡。

## 系统监控

在一个分布式系统中，对服务器的监控十分重要。FastOSS使用了Prometheus来收集和记录存储服务器的监控指标。

