# FastOSS 分布式对象存储系统

![](https://images-1257369645.cos.ap-chengdu.myqcloud.com/FastOSS/FastOSS-overview.png)



## 系统架构介绍

采用中间控制节点架构，由中间服务器控制集群。

- **Tracker**：中间控制节点，负责管理对象的存储位置和存储桶权限等信息。
- **Storage**：存储节点，存储对象数据以及对象的元数据。
- **Proxy**：代理服务器，负责接收HTTP请求并转发到系统中对应的处理服务器。Proxy服务器是无状态的，可以通过Nginx做负载均衡。
- **Zookeeper**：使用Zookeeper来管理Storage服务器，解除Tracker和Storage的耦合，避免Tracker管理大量的Storage服务器连接和心跳。

## Tracker服务

Tracker记录了对象存储系统中每个对象的具体位置（在哪台Storage服务器上），同时它也作为存储桶访问权限的认证中心。

### BitCask存储模型

为了保证不丢失对象的位置信息，Tracker使用了BitCask模型来存储KV数据。

![](https://images-1257369645.cos.ap-chengdu.myqcloud.com/FastOSS/bitCask%E5%AD%98%E5%82%A8%E6%A8%A1%E5%9E%8B.png)

### 一致性Hash负载均衡

在选择主副本存储位置时，Tracker服务器目前使用一致性Hash算法来保证Storage负载均衡。在后续的开发中将陆续引入轮询、加权轮询等更多的负载均衡算法。

### 存储桶访问权限

对象存储不具有文件系统的目录层级结构，不过用户可以使用存储桶来集中管理对象。在FastOSS中，存储桶有三种访问权限设置：

- 私有（Private）：对存储桶的读写全部是私有操作。
- 公共读（Public_Read）：存储桶开放读请求，写请求为私有。
- 公共写（Public_Write）：存储桶完全开放读写请求。

每个存储桶在创建后会生成唯一的AppID、AccessKey和SecretKey。用户通过这三个字符串来访问存储桶。

对于私有请求，用户需要通过SDK的Token工具，根据AccessKey和SecretKey生成访问Token，并在请求中携带Token。



## Storage服务

Storage存储了对象的数据和元数据。

### 大量小文件存储优化

由于系统的定位是存储大量小文件，所以Storage服务对小文件存储做了优化。

操作系统对每一个文件都会记录一份元数据，也就是Linux中的inode，其中包括权限组、用户组等信息。这些信息在对象存储中没有任何意义，但是会占据磁盘空间。在海量小文件的情境下，大量的文件系统元数据会占用大量磁盘资源。

为了解决该问题，FastOSS将小文件合并成大文件（Chunk），并规定每个大文件大小固定。单个的小文件都会已追加的形式写入大文件。通过这种方式减少额外的元数据存储，提升资源利用率。

删除文件时只需要在内存标记删除，后台会有专门的线程来压缩大文件中被删除的部分。

![](https://images-1257369645.cos.ap-chengdu.myqcloud.com/FastOSS/chunk.png)



## Proxy服务

Proxy类似存储系统的网关，它负责接收外部传来的Http请求，然后通过与系统内部的其他服务器交互请求完成处理，最终返回Http回复。

因为Proxy服务是无状态的，所以可以部署多台Proxy服务器并使用Nginx等工具做负载均衡。

对于热点数据，比如多次访问的Object的位置、多次访问的存储桶的权限，Proxy服务器会进行缓存，通过缓存来提高访问速度。