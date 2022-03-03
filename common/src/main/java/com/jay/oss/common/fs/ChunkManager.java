package com.jay.oss.common.fs;

import com.jay.oss.common.config.OssConfigs;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.*;
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
@Slf4j
public final class ChunkManager {
    /**
     * chunkSizeMap
     * key为chunk剩余空间大小，value为该空间大小的所有chunk阻塞队列
     */
    private final SortedMap<Long, BlockingQueue<Chunk>> chunkSizeMap = Collections.synchronizedSortedMap(new TreeMap<>());

    private final ConcurrentHashMap<Integer, Chunk> chunkMap = new ConcurrentHashMap<>();
    /**
     * chunk id provider
     */
    private final AtomicInteger chunkIdProvider = new AtomicInteger(1);

    private final Object mutex = new Object();
    /**
     * 获取一个大小足够容纳FileMeta的chunk
     * @param size file size
     * @return {@link Chunk}
     */
    public Chunk getChunkBySize(long size){
        // 获取所有的剩余大小比当前文件大小大的chunk队列
        SortedMap<Long, BlockingQueue<Chunk>> tailMap = chunkSizeMap.tailMap(size);
        Chunk chunk;
        /*
            逆向遍历所有剩余大小大于等于该文件的chunk。
            这样使文件落在chunk剩余大小较多的位置，使chunk大小分布更均匀
         */
        ArrayList<BlockingQueue<Chunk>> chunkQueues = new ArrayList<>(tailMap.values());
        for (int i = chunkQueues.size() - 1; i >= 0; i--) {
            BlockingQueue<Chunk> queue = chunkQueues.get(i);
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
        if((queue = chunkSizeMap.get(Chunk.MAX_CHUNK_SIZE - chunk.size())) == null){
            synchronized (mutex){
                if((queue = chunkSizeMap.get((long)chunk.size())) == null){
                    queue = new LinkedBlockingQueue<>();
                    chunkSizeMap.put(Chunk.MAX_CHUNK_SIZE - chunk.size(), queue);
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

    public List<Chunk> listChunks(){
        return new ArrayList<>(chunkMap.values());
    }

    /**
     * 从文件系统加载chunk信息
     * 最终记录在内存中
     */
    public void loadChunk(){
        File file = new File(OssConfigs.dataPath());
        File[] chunkFiles = file.listFiles(((dir, name) -> name.startsWith("chunk_")));
        if(chunkFiles == null){
            log.info("no chunk file found, skipping chunk loading");
            return;
        }
        long start = System.nanoTime();
        int count = 0;
        // 遍历chunk目录
        for(File chunkFile : chunkFiles){
            if(!chunkFile.isDirectory()){
                // 解析chunkID
                String name = chunkFile.getName();
                int chunkId = Integer.parseInt(name.substring(name.indexOf("_") + 1));
                // 创建chunk对象
                Chunk chunk = new Chunk(chunkFile.getPath(), chunkFile, chunkId);
                // 添加到chunkManager
                this.offerChunk(chunk);
                count ++;
            }
        }
        log.info("load chunk finished, loaded: {} chunks, time used: {} ms", count, (System.nanoTime() - start)/(1000000));
    }
}
