package com.jay.oss.common.bitcask;

import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.util.CompressUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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
     * writeLock
     */
    private final Object writeLock = new Object();

    /**
     * 存储名称
     * 系统中可以存在多个BitCask引擎，每个存储通过名称区分
     */
    private final String name;

    private final AtomicInteger chunkIdProvider = new AtomicInteger(0);

    public BitCaskStorage(String name) {
        this.name = name;
    }

    public void init() throws Exception {
        String path = OssConfigs.dataPath() + "/chunks";
        File directory = new File(path);
        // 加载文件目录下的属于该存储的chunk文件
        File[] files = directory.listFiles((dir, fileName) -> fileName.startsWith(name + "_chunk_"));
        if(files != null){
            for(File chunkFile : files){
                // 从文件创建chunk instance
                Chunk chunk = Chunk.getChunkInstance(chunkFile);
                if(chunk != null){
                    // 添加到chunk集合
                    chunks.add(chunk);
                }
            }
            chunks.sort(Comparator.comparingInt(Chunk::getChunkId));
        }
        log.info("BitCask Storage for {} loaded {} chunks", name, chunks.size());
    }

    public Index getIndex(String key){
        return indexCache.get(key);
    }

    public void saveIndex(String key, Index index){
        indexCache.put(key, index);
    }

    /**
     * get value
     * @param key key
     * @return byte[]
     * @throws IOException chunk读取异常
     */
    public byte[] get(String key) throws IOException {
        Index index = indexCache.get(key);
        Chunk chunk;
        if(index  != null && index.getChunkId() < chunks.size() && (chunk = chunks.get(index.getChunkId())) != null){
            byte[] content = chunk.read(index.getOffset());
            return content != null ? CompressUtil.decompress(content) : null;
        }else if(index != null){
            log.info("unknown chunk id: {}", index.getChunkId());
        }
        return null;
    }

    /**
     * put value
     * @param key key
     * @param value value byte[]
     * @throws IOException chunk写入异常
     */
    public boolean put(String key, byte[] value) throws IOException {
        synchronized (writeLock){
            if(!indexCache.containsKey(key)){
                byte[] keyBytes = key.getBytes(OssConfigs.DEFAULT_CHARSET);
                if(this.activeChunk == null || !activeChunk.isWritable()){
                    this.activeChunk = new Chunk(name, false, chunkIdProvider.getAndIncrement());
                    chunks.add(activeChunk);
                }
                byte[] compressedValue = CompressUtil.compress(value);
                int offset = activeChunk.write(keyBytes, compressedValue);
                Index index = new Index(activeChunk.getChunkId(), offset, false);
                indexCache.put(key, index);
                return true;
            }
            return false;
        }
    }

    /**
     * delete key
     * @param key key
     */
    public void delete(String key){
        indexCache.computeIfPresent(key, (k,v)->{
            v.setRemoved(true);
            return v;
        });
    }

    /**
     * merge old chunk
     * @throws IOException merge异常
     */
    public void merge() throws IOException {
        synchronized (writeLock){
            Chunk mergedChunk = new Chunk(name,true, 0);
            for (Map.Entry<String, Index> entry : indexCache.entrySet()) {
                String key = entry.getKey();
                Index index = entry.getValue();
                if(!index.isRemoved()){
                    // 获取原来的chunk
                    Chunk chunk = chunks.get(index.getChunkId());
                    if(chunk != null){
                        // 从原来的chunk读取数据
                        byte[] content = chunk.read(index.getOffset());
                        // 写入新chunk
                        int offset = mergedChunk.write(key.getBytes(OssConfigs.DEFAULT_CHARSET), content);
                        // 重置index的offset和chunkId
                        index.setOffset(offset);
                        index.setChunkId(0);
                    }
                }
            }
            this.activeChunk = mergedChunk;
        }
    }

    public void completeMerge() throws IOException {
        // 内存中删除其他chunk的对象
        Iterator<Chunk> iterator = chunks.iterator();
        while(iterator.hasNext()){
            Chunk chunk = iterator.next();
            chunk.closeChannel();
            iterator.remove();
        }
        // 删除被无效的index
        for (String key : indexCache.keySet()) {
            if(indexCache.get(key).isRemoved()){
                indexCache.remove(key);
            }
        }
        // 删除已经合并完成的chunk文件
        String path = OssConfigs.dataPath() + "/chunks";
        File directory = new File(path);
        File[] files = directory.listFiles((dir, fileName) -> fileName.startsWith(name + "_chunk_"));
        if(files != null){
            for(File chunkFile : files){
                if(!chunkFile.delete()){
                    log.warn("failed to delete chunk file");
                }
            }
        }
        // 重置activeChunk
        resetActiveChunk();
    }

    /**
     * 重置activeChunk
     * @throws IOException IOException
     */
    private void resetActiveChunk() throws IOException {
        if(this.activeChunk != null){
            String path = OssConfigs.dataPath() + "/chunks/" + name + "_merged_chunks";
            File file = new File(path);
            File chunk0 = new File(OssConfigs.dataPath() + "/chunks/" + name + "_chunk_0");
            this.activeChunk.closeChannel();
            if(file.renameTo(chunk0)){
                RandomAccessFile rf = new RandomAccessFile(chunk0, "rw");
                this.activeChunk.resetChannel(rf.getChannel());
                chunks.add(activeChunk);
                file.delete();
            }else {
                log.error("Failed to Reset Active Chunk");
            }
        }
    }

    public List<Index> listIndex(){
        return new ArrayList<>(indexCache.values());
    }
}
