package com.jay.oss.fs;

import com.jay.oss.entity.FileMeta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 *  为了优化大量小文件在磁盘的存储，FastOss将多个小文件合并为一个Chunk。
 *  ChunkManager负责管理chunk文件
 * </p>
 *
 * @author Jay
 * @date 2022/01/17 15:44
 */
public final class ChunkManager {
    /**
     * chunkSizeMap
     * key为chunk剩余空间大小，value为该空间大小的所有chunk阻塞队列
     */
    private final SortedMap<Integer, BlockingQueue<Chunk>> chunkSizeMap = Collections.synchronizedSortedMap(new TreeMap<>());

    private final ConcurrentHashMap<Integer, Chunk> chunkMap = new ConcurrentHashMap<>();
    /**
     * chunk id provider
     */
    private final AtomicInteger chunkIdProvider = new AtomicInteger(1);

    private final Object mutex = new Object();
    /**
     * 获取一个大小足够容纳FileMeta的chunk
     * @param meta {@link FileMeta}
     * @return {@link Chunk}
     */
    public Chunk getChunkBySize(FileMeta meta){
        // 获取所有的剩余大小比当前文件大小大的chunk队列
        SortedMap<Integer, BlockingQueue<Chunk>> tailMap = chunkSizeMap.tailMap(meta.getSize());
        Chunk chunk;
        // 遍历有足够空间的chunk
        ArrayList<BlockingQueue<Chunk>> chunkQueues = new ArrayList<>(tailMap.values());
        for (BlockingQueue<Chunk> queue : chunkQueues) {
            if((chunk = queue.poll()) != null){
                return chunk;
            }
        }
        // 没有chunk，创建新chunk
        return createChunkAndGet();
    }

    /**
     * 向chunkManager添加chunk
     * @param chunk {@link Chunk}
     */
    public void offerChunk(Chunk chunk){
        BlockingQueue<Chunk> queue;
        // 保证线程安全，只有一个线程能创建新的queue
        if((queue = chunkSizeMap.get(Chunk.MAX_CHUNK_SIZE - chunk.getSize())) == null){
            synchronized (mutex){
                if((queue = chunkSizeMap.get(chunk.getSize())) == null){
                    queue = new LinkedBlockingQueue<>();
                    chunkSizeMap.put(Chunk.MAX_CHUNK_SIZE - chunk.getSize(), queue);
                }
            }
        }
        // 添加chunk到queue中
        queue.add(chunk);
        chunkMap.putIfAbsent(chunk.getId(), chunk);
    }

    /**
     * 获取chunk
     * @param chunkId chunk id
     * @return {@link Chunk} null if no such id
     */
    public Chunk getChunkById(Integer chunkId){
        return chunkMap.get(chunkId);
    }

    /**
     * 创建并获取chunk
     * @return {@link Chunk}
     */
    public Chunk createChunkAndGet(){
        int id = chunkIdProvider.getAndIncrement();
        return new Chunk(id);
    }
}
