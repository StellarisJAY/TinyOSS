package com.jay.oss.tracker.track;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.jay.oss.common.entity.object.ObjectMeta;
import com.jay.oss.common.kv.KvStorage;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.common.util.StringUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

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
     * 热点元数据缓存
     */
    private final Cache<String, ObjectMeta> cache = Caffeine.newBuilder()
            .expireAfterAccess(60, TimeUnit.MINUTES)
            .maximumSize(100000)
            .recordStats().build();
    /**
     * 元数据磁盘存储
     */
    private final KvStorage metaStorage;

    public ObjectTracker(KvStorage metaStorage) {
        this.metaStorage = metaStorage;
    }

    /**
     * 定位object在哪些主机上
     * @param objectKey objectKey
     * @return Url String
     */
    public String locateObject(String objectKey){
        ObjectMeta objectMeta = getObjectMeta(objectKey);
        return objectMeta == null ? null : objectMeta.getLocations() + objectMeta.getObjectId();
    }

    /**
     * 读取object元数据
     * @param objectKey objectKey
     * @return {@link ObjectMeta}
     */
    public ObjectMeta getObjectMeta(String objectKey)  {
        try{
            ObjectMeta object = cache.getIfPresent(objectKey);
            if(object != null){
                return object;
            }else{
                byte[] serialized = metaStorage.get(objectKey);
                if(serialized != null && serialized.length > 0){
                    ObjectMeta meta = SerializeUtil.deserialize(serialized, ObjectMeta.class);
                    cache.put(objectKey, meta);
                    return meta;
                }
                return null;
            }
        }catch (Exception e){
            log.error("Get Object Meta Failed, meta: {}", objectKey, e);
            return null;
        }
    }

    /**
     * 保存object元数据
     * @param meta {@link ObjectMeta}
     * @return boolean 保存是否成功，如果是重复的key会导致返回false
     */
    public boolean putObjectMeta(String objectKey, ObjectMeta meta)  {
        byte[] serialized = SerializeUtil.serialize(meta, ObjectMeta.class);
        return metaStorage.putIfAbsent(objectKey, serialized);
    }

    public boolean putObjectId(long objectId, String objectKey){
        String id = Long.toString(objectId);
        return metaStorage.putIfAbsent(id, StringUtil.getBytes(objectKey));
    }

    public ObjectMeta getMetaById(String objectId){
        ObjectMeta meta = cache.getIfPresent(objectId);
        if(meta != null){
            return meta;
        }
        try{
            byte[] objectKey = metaStorage.get(objectId);
            if(objectKey != null){
                return getObjectMeta(StringUtil.toString(objectKey));
            }
            return null;
        }catch (Exception e){
            return null;
        }
    }

    public ObjectMeta deleteObjectMeta(String objectKey){
        ObjectMeta meta = getObjectMeta(objectKey);
        if(meta != null && metaStorage.delete(objectKey)){
            return meta;
        }
        return null;
    }
}
