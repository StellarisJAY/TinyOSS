package com.jay.oss.common.bitcask;

import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.util.CompressUtil;
import com.jay.oss.common.util.StringUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * <p>
 *  BitCask存储引擎
 * </p>
 *
 * @author Jay
 * @date 2022/03/04 10:55
 */
@Slf4j
public class BitCaskStorage {
    /**
     * Index Hash表
     */
    private final ConcurrentHashMap<String, Index> indexCache = new ConcurrentHashMap<>();

    /**
     * chunk列表，列表下标和chunkId对应
     */
    private final List<Chunk> chunks = new ArrayList<>();

    /**
     * 当前活跃chunk
     */
    private Chunk activeChunk = null;

    /**
     * Hint 日志
     */
    private HintLog hintLog;

    private static final byte[] DELETE_TAG = new byte[]{(byte)127};

    /**
     * writeLock
     */
    private final Object activeChunkLock = new Object();

    /**
     * 压缩old chunk的读写锁
     */
    private final ReentrantReadWriteLock compactLock = new ReentrantReadWriteLock();


    private final AtomicInteger chunkIdProvider = new AtomicInteger(0);

    public static final String CHUNK_DIRECTORY = File.separator + "chunks";

    /**
     * 初始化BitCask存储模型
     * 首先会加载目录下的chunk文件，然后读取Hint日志来加载索引信息。
     * @throws Exception e
     */
    public void init() throws Exception {
        String path = OssConfigs.dataPath() + CHUNK_DIRECTORY;
        File directory = new File(path);
        // 加载文件目录下的属于该存储的chunk文件
        File[] files = directory.listFiles((dir, fileName) -> fileName.startsWith("chunk_"));
        if(files != null){
            for(File chunkFile : files){
                Chunk chunk = Chunk.getChunkInstance(chunkFile);
                if(chunk != null){
                    chunks.add(chunk);
                }
            }
            chunks.sort(Comparator.comparingInt(Chunk::getChunkId));
        }
        // 加载索引日志
        hintLog = new HintLog();
        hintLog.init();
        log.info("BitCask Storage loaded {} chunks", chunks.size());
    }

    public Index getIndex(String key){
        return indexCache.get(key);
    }

    public void saveIndex(String key, Index index){
        indexCache.put(key, index);
    }

    /**
     * 保证activeChunk可写入
     * @throws IOException e
     */
    private void ensureActiveChunk() throws IOException {
        if(this.activeChunk == null || !activeChunk.isWritable()){
            // 创建新的active chunk
            this.activeChunk = new Chunk(false, chunkIdProvider.getAndIncrement());
            chunks.add(activeChunk);
        }
    }

    /**
     * get value
     * @param key key
     * @return byte[]
     * @throws IOException chunk读取异常
     */
    public byte[] get(String key) throws IOException {
        try{
            compactLock.readLock().lock();
            Index index = indexCache.get(key);
            Chunk chunk;
            if(index  != null && !index.isRemoved() && index.getChunkId() < chunks.size() && (chunk = chunks.get(index.getChunkId())) != null){
                return chunk.read(index.getOffset());
            }else if(index != null){
                log.info("unknown chunk id: {}", index.getChunkId());
            }
            return null;
        } finally {
            compactLock.readLock().unlock();
        }

    }

    /**
     * put value
     * @param key key
     * @param value value byte[]
     */
    public boolean put(String key, byte[] value)  {
        if(!indexCache.containsKey(key)){
            synchronized (activeChunkLock){
                try{
                    if(!indexCache.containsKey(key)){
                        ensureActiveChunk();
                        byte[] keyBytes = StringUtil.getBytes(key);
                        int offset = activeChunk.write(keyBytes, value);
                        Index index = new Index(activeChunk.getChunkId(), offset, false);
                        indexCache.put(key, index);
                        return true;
                    }
                    return false;
                }catch (IOException e){
                    return false;
                }
            }
        }
        return false;
    }

    /**
     * 更新值
     * @param key key
     * @param value value
     * @return boolean
     * @throws IOException IOException
     */
    public boolean update(String key, byte[] value) throws IOException {
        synchronized (activeChunkLock){
            byte[] keyBytes = key.getBytes(OssConfigs.DEFAULT_CHARSET);
            if(this.activeChunk == null || !activeChunk.isWritable()){
                // 创建新的active chunk
                this.activeChunk = new Chunk(false, chunkIdProvider.getAndIncrement());
                chunks.add(activeChunk);
            }
            byte[] compressedValue = CompressUtil.compress(value);
            int offset = activeChunk.write(keyBytes, compressedValue);
            Index index = new Index(activeChunk.getChunkId(), offset, false);
            indexCache.put(key, index);
            return true;
        }
    }

    /**
     * delete key
     * @param key key
     * @return boolean
     */
    public boolean delete(String key){
        if(indexCache.containsKey(key)){
            synchronized (activeChunkLock){
                try{
                    Index index = indexCache.get(key);
                    if(index != null){
                        index.setRemoved(true);
                        ensureActiveChunk();
                        activeChunk.write(StringUtil.getBytes(key), DELETE_TAG);
                        return true;
                    }
                }catch (IOException e){
                    log.warn("BitCask Delete failed, key: {}", key, e);
                    return false;
                }
            }
        }
        return false;
    }

    /**
     * 压缩、合并chunk文件
     * 1、读取所有索引，把没被删除的数据重新写到新的chunk中。
     * 2、修改index的offset和chunkId
     * 3、把index写入HintLog
     */
    public void compact(){
        try{
            // 加锁，避免有其他线程读取
            compactLock.writeLock().lock();
            Chunk mergedChunk = new Chunk(true, 0);
            for (Map.Entry<String, Index> entry : indexCache.entrySet()) {
                String key = entry.getKey();
                Index index = entry.getValue();
                if(!index.isRemoved() && index.getChunkId() != activeChunk.getChunkId()){
                    byte[] keyBytes = StringUtil.getBytes(key);
                    // 从旧的chunk读取value
                    byte[] value = this.get(key);
                    // 写入新chunk并更新index
                    int offset = mergedChunk.write(keyBytes, value);
                    index.setChunkId(0);
                    index.setOffset(offset);
                    // 写入Hint File
                    hintLog.append(new HintIndex(key, index.getChunkId(), index.getOffset(), false));
                }
            }
            // 强制Hint Log刷盘
            hintLog.swapBuffer(true);
        }catch (IOException e){
            log.warn("Compact BitCask chunk failed ", e);
        }
        finally {
            compactLock.writeLock().unlock();
        }
    }

    public List<String> keys(){
        return new ArrayList<>(indexCache.keySet());
    }

}
