package com.jay.oss.tracker.track;

import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.tracker.track.bitcask.Chunk;
import com.jay.oss.tracker.track.bitcask.ObjectIndex;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
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
            throw new RuntimeException("can't locate object");
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
                    activeChunk = new Chunk();
                    chunks.add(activeChunk);
                }
                // 写入数据
                int offset = activeChunk.write(keyBytes, urlBytes);
                // 记录索引
                ObjectIndex index = new ObjectIndex(activeChunk.getChunkId(), offset, false);
                indexCache.put(objectKey, index);
            }catch (Exception e){
                log.error("Failed to save object location ", e);
            }
        }
    }
}
