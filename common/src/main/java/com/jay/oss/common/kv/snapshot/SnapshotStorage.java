package com.jay.oss.common.kv.snapshot;

import com.jay.dove.util.NamedThreadFactory;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.kv.KvStorage;
import com.jay.oss.common.util.Scheduler;
import com.jay.oss.common.util.StringUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * <p>
 *  快照存储引擎，不建议在非开发场景使用
 *  所有的kv都存储在内存中，并以快照的形式定期持久化
 *  该存储引擎仅支持少量的KV，且不保证持久性
 * </p>
 *
 * @author Jay
 * @date 2022/05/05 14:18
 */
@Slf4j
public class SnapshotStorage implements KvStorage {

    private final ConcurrentHashMap<String, byte[]> memTable = new ConcurrentHashMap<>(256);
    private final ByteBuf snapshotBuffer = Unpooled.directBuffer();
    private long lastFlushTime;

    private final Object flushLock = new Object();
    private static final long FLUSH_INTERVAL = 10 * 1000;
    private final ExecutorService flushExecutor = new ThreadPoolExecutor(1, 1, 0,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(3),
            new NamedThreadFactory("snapshot-flush-thread-", false),
            new ThreadPoolExecutor.DiscardPolicy());
    private Snapshot lastSnapshot = null;

    @Override
    public void init() throws Exception {
        lastFlushTime = System.currentTimeMillis();
        String dataPath = OssConfigs.dataPath();
        File directory = new File(dataPath);

        File lastFile = Arrays.stream(Objects.requireNonNull(directory.listFiles((dir, name) -> name.startsWith("snapshot-"))))
                .sorted(Comparator.comparing(File::getName))
                .collect(Collectors.toList())
                .get(0);
        lastSnapshot = new Snapshot(lastFile);
        memTable.putAll(lastSnapshot.read());
        // 定时刷盘
        Scheduler.scheduleAtFixedRate(this::flushSnapshot, FLUSH_INTERVAL, FLUSH_INTERVAL, TimeUnit.MILLISECONDS);
    }

    @Override
    public byte[] get(String key) throws Exception {
        return memTable.get(key);
    }

    @Override
    public boolean putIfAbsent(String key, byte[] value) {
        tryFlushSnapshot();
        return memTable.putIfAbsent(key, value) == null;
    }

    @Override
    public boolean put(String key, byte[] value) {
        tryFlushSnapshot();
        memTable.put(key, value);
        return true;
    }

    @Override
    public boolean delete(String key) {
        tryFlushSnapshot();
        return memTable.remove(key) == null;
    }

    @Override
    public List<String> keys() {
        return new ArrayList<>(memTable.keySet());
    }

    @Override
    public boolean containsKey(String key) {
        return memTable.containsKey(key);
    }

    private void tryFlushSnapshot(){
        if (System.currentTimeMillis() - lastFlushTime > FLUSH_INTERVAL){
            flushSnapshot();
        }
    }

    private void flushSnapshot(){
        synchronized (flushLock){
            if(System.currentTimeMillis() - lastFlushTime > FLUSH_INTERVAL){
                // 把memTable里面的kv写入buffer
                for (Map.Entry<String, byte[]> entry : memTable.entrySet()) {
                    byte[] keyBytes = StringUtil.getBytes(entry.getKey());
                    snapshotBuffer.writeInt(keyBytes.length);
                    snapshotBuffer.writeInt(entry.getValue().length);
                    snapshotBuffer.writeBytes(keyBytes);
                    snapshotBuffer.writeBytes(entry.getValue());
                }
                lastFlushTime = System.currentTimeMillis();
                // 异步刷盘
                flushExecutor.submit(()->{
                    try{
                        // 创建新的快照，写入快照文件
                        Snapshot snapshot = new Snapshot();
                        snapshot.write(snapshotBuffer.nioBuffer());
                        if(lastSnapshot != null){
                            // 删除旧的快照文件
                            lastSnapshot.delete();
                        }
                        lastSnapshot = snapshot;
                    }catch (Exception e){
                        log.warn("Failed to flush snapshot " ,e);
                    }
                });
            }
        }
    }
}
