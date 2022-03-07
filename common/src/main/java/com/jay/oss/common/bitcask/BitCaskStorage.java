package com.jay.oss.common.bitcask;

import com.jay.oss.common.config.OssConfigs;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

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
                log.info("chunk file: {}", chunkFile);
                // 从文件创建chunk instance
                Chunk chunk = Chunk.getChunkInstance(chunkFile);
                if(chunk != null){
                    // 添加到chunk集合
                    chunks.add(chunk);
                }
            }
            chunks.sort(Comparator.comparingInt(Chunk::getChunkId));
        }
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
        if(index  != null && (chunk = chunks.get(index.getChunkId())) != null){
            return chunk.read(index.getOffset());
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
                    this.activeChunk = new Chunk(name, false);
                    chunks.add(activeChunk);
                }
                int offset = activeChunk.write(keyBytes, value);
                Index index = new Index(key, activeChunk.getChunkId(), offset, false);
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
            Chunk mergedChunk = new Chunk(name,true);
            for (Index index : indexCache.values()) {
                String key = index.getKey();
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
            // 内存中删除其他chunk的对象
            Iterator<Chunk> iterator = chunks.iterator();
            while(iterator.hasNext()){
                Chunk chunk = iterator.next();
                chunk.closeChannel();
                iterator.remove();
            }
            this.activeChunk = mergedChunk;
        }
    }

    public void completeMerge() throws IOException {
        // 删除被无效的index
        for (String key : indexCache.keySet()) {
            if(indexCache.get(key).isRemoved()){
                indexCache.remove(key);
            }
        }
        // 删除已经合并完成的chunk文件
        String path = OssConfigs.dataPath() + "/chunks";
        File directory = new File(path);
        File[] files = directory.listFiles((dir, name) -> name.startsWith(name + "_chunk_"));
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

    private void resetActiveChunk() throws IOException {
        if(this.activeChunk != null){
            String path = OssConfigs.dataPath() + "/chunks/" + name + "_merged_chunk";
            File file = new File(path);
            File chunk0 = new File(OssConfigs.dataPath() + "/chunks/" + name + "_chunk_0");
            if(!chunk0.exists() && !chunk0.createNewFile()){
                throw new RuntimeException("can't move merged chunks into chunk0");
            }
            RandomAccessFile raf = new RandomAccessFile(chunk0, "rw");
            FileChannel chunk0Channel = raf.getChannel();
            long transferred = this.activeChunk.getActiveChannel().transferTo(0, activeChunk.getSize(), chunk0Channel);
            log.info("transferred: {}", transferred);
            this.activeChunk.closeChannel();
            this.activeChunk.resetChannel(chunk0Channel);
            chunks.add(this.activeChunk);
            file.delete();
        }
    }

    public List<Index> listIndex(){
        return new ArrayList<>(indexCache.values());
    }
}
