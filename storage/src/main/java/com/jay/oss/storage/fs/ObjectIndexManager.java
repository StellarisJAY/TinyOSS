package com.jay.oss.storage.fs;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * <p>
 *  对象索引管理器
 *  负责存储对象的Block索引
 * </p>
 *
 * @author Jay
 * @date 2022/01/18 14:48
 */
public class ObjectIndexManager {
    private final ConcurrentHashMap<Long, ObjectIndex> indexCache = new ConcurrentHashMap<>(256);

    /**
     * 互斥执行操作
     * @param objectId 对象ID
     * @param function {@link Function}
     */
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
