package com.jay.oss.storage.fs;

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
 *
 * </p>
 *
 * @author Jay
 * @date 2022/04/13 14:07
 */
public class BlockManager {
    /**
     * blockSizeMap
     * key为block剩余空间大小，value为该空间大小的所有block阻塞队列
     */
    private final SortedMap<Integer, BlockingQueue<Block>> blockSizeMap = Collections.synchronizedSortedMap(new TreeMap<>());

    private final ConcurrentHashMap<Integer, Block> blockMap = new ConcurrentHashMap<>();

    /**
     * 临时block集合
     * 分片上传时存储分片的临时block
     */
    private final ConcurrentHashMap<String, Block> tempBlockMap = new ConcurrentHashMap<>();

    /**
     * block id provider
     */
    private final AtomicInteger blockIdProvider = new AtomicInteger(1);

    private final Object mutex = new Object();

    /**
     * 获取一个大小足够容纳FileMeta的block
     * @param size file size
     * @return {@link Block}
     */
    public Block getBlockBySize(int size){
        // 获取所有的剩余大小比当前文件大小大的block队列
        SortedMap<Integer, BlockingQueue<Block>> tailMap = blockSizeMap.tailMap(size);
        Block block;
        /*
            逆向遍历所有剩余大小大于等于该文件的block。
            这样使文件落在block剩余大小较多的位置，使block大小分布更均匀
         */
        ArrayList<BlockingQueue<Block>> blockQueues = new ArrayList<>(tailMap.values());
        for (int i = blockQueues.size() - 1; i >= 0; i--) {
            BlockingQueue<Block> queue = blockQueues.get(i);
            if((block = queue.poll()) != null){
                return block;
            }
        }
        // 没有block，创建新block
        return createBlockAndGet();
    }

    /**
     * 向blockManager添加block
     * @param block {@link Block}
     */
    public void offerBlock(Block block){
        BlockingQueue<Block> queue;
        int availableSpace = block.availableSpace();
        // 保证线程安全，只有一个线程能创建新的queue
        if((queue = blockSizeMap.get(availableSpace)) == null){
            synchronized (mutex){
                if((queue = blockSizeMap.get(availableSpace)) == null){
                    queue = new LinkedBlockingQueue<>();
                    blockSizeMap.put(availableSpace, queue);
                }
            }
        }
        // 添加block到queue中
        queue.add(block);
        blockMap.putIfAbsent(block.getId(), block);
    }

    /**
     * 获取block
     * @param blockId block id
     * @return {@link Block} null if no such id
     */
    public Block getBlockById(Integer blockId){
        return blockMap.get(blockId);
    }

    /**
     * 创建并获取block
     * @return {@link Block}
     */
    public Block createBlockAndGet(){
        int id = blockIdProvider.getAndIncrement();
        return new Block(id);
    }
}
