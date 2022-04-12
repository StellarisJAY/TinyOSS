# BitCask读写性能测试

## 测试环境

- CPU：Intel Core i7-8750H 2.2GHz
- 内存：16GB
- JVM：OpenJDK 17

## 测试1（单线程读写）

单线程读写100万个键值对

### 测试代码

```java
@Test
    public void testIOSpeed() throws Exception {
        // 初始化
        BitCaskStorage bitCaskStorage = new BitCaskStorage();
        bitCaskStorage.init();
        // 序列化value
        Serializer serializer = new ProtostuffSerializer();
        byte[] value = serializer.serialize(objectMeta, ObjectMeta.class);
        String keyPrefix = "object";

        // 写入100万个KV对
        int total = 1000000;
        long putStart = System.currentTimeMillis();
        for(int i = 0; i < total; i++){
            bitCaskStorage.put(keyPrefix + i + ".png", value);
        }
        long wTime = System.currentTimeMillis() - putStart;
        log.info("Write time used: {}ms, written: {} kv, speed: {} /s", wTime,total, (total * 1000) / wTime);


        // 读取100万个KV对
        long getStart = System.currentTimeMillis();
        for(int i = 0; i < total; i++){
            bitCaskStorage.get(keyPrefix + i + ".png");
        }
        long rTime = System.currentTimeMillis() - getStart;
        log.info("Read time used: {}ms, read: {} kv, speed: {} /s", rTime,total, (total * 1000) / rTime);
    }
```

### 三次测试结果

```
 - Write time used: 652ms, written: 1000000 kv, speed: 1533742 /s
 - Read time used: 380ms, read: 1000000 kv, speed: 2631578 /s
```

```
 - Write time used: 817ms, written: 1000000 kv, speed: 1223990 /s
 - Read time used: 389ms, read: 1000000 kv, speed: 2570694 /s
```

```
 - Write time used: 756ms, written: 1000000 kv, speed: 1322751 /s
 - Read time used: 387ms, read: 1000000 kv, speed: 2583979 /s
```

写入平均速度：1360161 kv / s

读取平均速度：2595417 kv / s

## 测试2（并发读写）

多线程并发读写

### 测试代码

```java
@Test
    public void testConcurrentIO() throws Exception {
        // 初始化，与单线程测试代码相同，省略

        // 线程数
        int threadCount = 1000;
        // 每个线程write的KV对数量
        int loop = 1000;
        int total = threadCount * loop;

        // 并发写入
        CountDownLatch writeCountDown = new CountDownLatch(threadCount);
        long putStart = System.currentTimeMillis();
        for(int i = 0; i < threadCount; i++){
            int threadNum = i;
            Thread thread = new Thread(() -> {
                for (int j = 0; j < loop; j++) {
                    bitCaskStorage.put(keyPrefix + threadNum + "-" + j, value);
                }
                writeCountDown.countDown();
            });
            thread.start();
        }
        writeCountDown.await();
        long wTime = System.currentTimeMillis() - putStart;
        log.info("Write time used: {}ms, written: {} kv, speed: {} /s", wTime,total, (total * 1000L) / wTime);

        // 并发读取
        CountDownLatch readCountDown = new CountDownLatch(threadCount);
        long readStart = System.currentTimeMillis();
        for(int i = 0; i < threadCount; i++){
            int threadNum = i;
            Thread thread = new Thread(() -> {
                for (int j = 0; j < loop; j++) {
                    bitCaskStorage.put(keyPrefix + threadNum + "-" + j, value);
                }
                readCountDown.countDown();
            });
            thread.start();
        }
        readCountDown.await();
        long rTime = System.currentTimeMillis() - readStart;
        log.info("Read time used: {}ms, read: {} kv, speed: {} /s", rTime,total, (total * 1000L) / rTime);
    }
```

### 三次测试结果

```
 - Write time used: 1022ms, written: 1000000 kv, speed: 978473 /s
 - Read time used: 204ms, read: 1000000 kv, speed: 4901960 /s
```

```
 - Write time used: 1013ms, written: 1000000 kv, speed: 987166 /s
 - Read time used: 209ms, read: 1000000 kv, speed: 4784688 /s
```

```
 - Write time used: 1002ms, written: 1000000 kv, speed: 998003 /s
 - Read time used: 226ms, read: 1000000 kv, speed: 4424778 /s
```

写入平均速度：987,880.7 kv / s

读取平均速度：4,703,808.7 kv / s

## 结果分析

- BitCask存储模型的所有写操作都是追加写，因此写操作必须串行化。这就是为什么在并发的情况下的写入的速度要远低于读取的速度，因为所有写入操作需要竞争同一个互斥锁，而读取获取的是分段的共享锁。
- FastOSS最新的BitCask实现中使用了mmap替换原来的FileChannel，原因是BitCask的键值对读写的数据是很小的，所以在该场景下性能瓶颈并不是IO，而是CPU内核态和用户态的切换。使用FileChannel的read和write方法都会导致系统调用，而mmap不会出现内核态和用户态切换，可以避免CPU因为切换状态不堪重负。

