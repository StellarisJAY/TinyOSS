package com.jay.oss.storage.meta;

import com.jay.oss.common.entity.FileMetaWithChunkInfo;
import com.jay.oss.storage.fs.ObjectIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
    private final ConcurrentHashMap<Long, ObjectIndex> indexCache = new ConcurrentHashMap<>(256);


    public void computeIfAbsent(long objectId, Function<Long, ?extends ObjectIndex> function){
        indexCache.computeIfAbsent(objectId, function);
    }

    public ObjectIndex getObjectIndex(long objectId){
        return indexCache.get(objectId);
    }

    public void deleteIndex(long objectId){
        indexCache.remove(objectId);
    }

    public void putIndexes(Map<Long, ObjectIndex> indexMap){
        this.indexCache.putAll(indexMap);
    }

}
