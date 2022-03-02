package com.jay.oss.tracker.track;

import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.tracker.track.bitcask.Chunk;
import com.jay.oss.tracker.track.bitcask.ObjectIndex;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 *  Object定位器
 *  使用BitCask存储模型存储object的位置信息
 * </p>
 *
 * @author Jay
 * @date 2022/02/21 10:09
 */
@Slf4j
public class ObjectTracker {
    /**
     * cache table
     * 热点数据缓存
     */
    private final ConcurrentHashMap<String, String> locationCache = new ConcurrentHashMap<>();
    /**
     * index cache
     */
    private final ConcurrentHashMap<String, ObjectIndex> indexCache = new ConcurrentHashMap<>();

    /**
     * chunk list
     */
    private final List<Chunk> chunks = new ArrayList<>();

    /**
     * 当前活跃chunk
     */
    private Chunk activeChunk = null;
    private final Object writeLock = new Object();

    /**
     * 初始化object tracker
     * 扫描chunk路径，获取已经存在的chunk文件
     * @throws Exception e
     */
    public void init() throws Exception {
        String path = OssConfigs.dataPath() + "/chunks";
        File directory = new File(path);
        File[] files = directory.listFiles();
        if(files != null){
            for(File chunkFile : files){
                // 从文件创建chunk instance
                Chunk chunk = Chunk.getChunkInstance(chunkFile);
                if(chunk != null){
                    // 添加到chunk集合
                    chunks.add(chunk.getChunkId(), chunk);
                }
            }
        }
    }

    public String locateObject(String objectKey){
        try{
            String urls = locationCache.get(objectKey);
            if(urls == null){
                ObjectIndex index = indexCache.get(objectKey);
                if(index != null){
                    Chunk chunk = chunks.get(index.getChunkId());
                    byte[] value = chunk.read(index.getOffset());
                    urls = new String(value, OssConfigs.DEFAULT_CHARSET);
                    locationCache.put(objectKey, urls);
                }
            }
            return urls;
        }catch (Exception e){
            log.warn("locate object failed, key: {}, ", objectKey, e);
            return null;
        }
    }

    /**
     * 保存object位置
     * @param objectKey objectKey
     * @param urls 位置urls
     */
    public void saveObjectLocation(String objectKey, String urls){
        synchronized (writeLock){
            try{
                byte[] keyBytes = objectKey.getBytes(OssConfigs.DEFAULT_CHARSET);
                byte[] urlBytes = urls.getBytes(OssConfigs.DEFAULT_CHARSET);
                // 检查当前chunk是否可写入
                if(activeChunk == null || !activeChunk.isWritable()){
                    // 创建新chunk
                    activeChunk = new Chunk(false);
                    chunks.add(activeChunk);
                }
                // 写入数据
                int offset = activeChunk.write(keyBytes, urlBytes);
                // 记录索引
                ObjectIndex index = new ObjectIndex(objectKey, activeChunk.getChunkId(), offset, false);
                indexCache.put(objectKey, index);
            }catch (Exception e){
                log.error("Failed to save object location ", e);
            }
        }
    }

    public ObjectIndex getIndex(String key){
        return indexCache.get(key);
    }

    public void saveObjectIndex(String key, ObjectIndex objectIndex){
        indexCache.put(key, objectIndex);
    }

    public void deleteObject(String key){
        indexCache.computeIfPresent(key, (k, value) -> {
            value.setRemoved(true);
            return value;
        });
    }

    /**
     * 合并chunk文件
     * 该操作在启动时会执行
     * 也会在空闲时由后台线程执行
     */
    public void merge() throws Exception {
        synchronized (writeLock){
            Chunk mergedChunk = new Chunk(true);
            for (ObjectIndex index : indexCache.values()) {
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
        }
    }

    public List<ObjectIndex> listIndexes(){
        return new ArrayList<>(indexCache.values());
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
        File[] files = directory.listFiles((dir, name) -> name.startsWith("chunk_"));
        if(files != null){
            for(File chunkFile : files){
                if(!chunkFile.delete()){
                    log.warn("failed to delete chunk file");
                }
            }
        }
        resetActiveChunk();
    }

    private void resetActiveChunk() throws IOException {
        this.activeChunk.closeChannel();
        String path = OssConfigs.dataPath() + "/chunks/merged_chunk";
        File file = new File(path);
        File renamed = new File(OssConfigs.dataPath() + "/chunks/chunk_0");
        if(!file.renameTo(renamed)){
            throw new RuntimeException("can't rename merged chunk file");
        }
        RandomAccessFile randomAccessFile = new RandomAccessFile(renamed, "rw");
        this.activeChunk.resetChannel(randomAccessFile.getChannel());
    }
}
