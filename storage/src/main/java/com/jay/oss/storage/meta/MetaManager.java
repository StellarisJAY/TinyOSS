package com.jay.oss.storage.meta;

import com.jay.oss.common.entity.FileMetaWithChunkInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * <p>
 *  元数据管理器
 *  负责存储元数据和桶信息。
 *  定时进行持久化操作，保证元数据安全
 * </p>
 *
 * @author Jay
 * @date 2022/01/18 14:48
 */
public class MetaManager {
    /**
     * 元数据缓存
     */
    private final ConcurrentHashMap<String, FileMetaWithChunkInfo> fileMetaCache = new ConcurrentHashMap<>(256);

    public boolean saveMeta(FileMetaWithChunkInfo meta){
        return fileMetaCache.putIfAbsent(meta.getKey(), meta) == null;
    }

    public void computeIfAbsent(String key, Function<String, ?extends FileMetaWithChunkInfo> function){
        fileMetaCache.computeIfAbsent(key, function);
    }

    public FileMetaWithChunkInfo delete(String key){
        return fileMetaCache.remove(key);
    }

    public boolean fileExists(String key){
        return fileMetaCache.containsKey(key);
    }

    public FileMetaWithChunkInfo getMeta(String key){
        return fileMetaCache.get(key);
    }

    public List<FileMetaWithChunkInfo> snapshot(){
        return new ArrayList<>(fileMetaCache.values());
    }
}
