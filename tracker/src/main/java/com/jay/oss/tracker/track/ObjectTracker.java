package com.jay.oss.tracker.track;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.jay.oss.common.entity.object.ObjectMeta;
import com.jay.oss.common.kv.KvStorage;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.common.util.StringUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
     * 对象位置缓存
     */
    private final Map<Long, Set<String>> objectLocations = new ConcurrentHashMap<>(256);
    /**
     * 元数据磁盘存储
     */
    private final KvStorage metaStorage;

    public ObjectTracker(KvStorage metaStorage) {
        this.metaStorage = metaStorage;
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
                String objectId = getObjectId(objectKey);
                if (!StringUtil.isNullOrEmpty(objectId)) {
                    byte[] serialized = metaStorage.get(objectId);
                    if(serialized != null && serialized.length > 0){
                        ObjectMeta meta = SerializeUtil.deserialize(serialized, ObjectMeta.class);
                        cache.put(objectKey, meta);
                        return meta;
                    }
                }
                return null;
            }
        }catch (Exception e){
            log.error("Get Object Meta Failed, meta: {}", objectKey, e);
            return null;
        }
    }

    public ObjectMeta getObjectMetaById(Long objectId) {
        String id = Long.toString(objectId);
        byte[] serialized = metaStorage.get(id);
        if (serialized != null && serialized.length > 0) {
            return SerializeUtil.deserialize(serialized, ObjectMeta.class);
        }
        return null;
    }

    public boolean isObjectDeleted(long objectId){
        return !metaStorage.containsKey(Long.toString(objectId));
    }

    /**
     * 通过objectKey获取objectId
     * @param objectKey objectKey
     * @return objectId
     */
    public String getObjectId(String objectKey) {
        byte[] idBytes = metaStorage.get(objectKey);
        return idBytes == null ? null : StringUtil.toString(idBytes);
    }

    public Set<String> getObjectLocations(String objectId){
        if(objectId != null && metaStorage.get(objectId) != null){
            return getObjectReplicaLocations(Long.parseLong(objectId));
        }
        return null;
    }

    /**
     * 保存对象元数据
     * @param objectKey objectKey
     * @param meta {@link ObjectMeta}
     * @return 保存是否成功，如果key重复返回false
     */
    public boolean putMeta(String objectKey, ObjectMeta meta){
        byte[] serialized = SerializeUtil.serialize(meta, ObjectMeta.class);
        String id = Long.toString(meta.getObjectId());
        if(metaStorage.putIfAbsent(objectKey, StringUtil.getBytes(id))){
            if(metaStorage.put(id, serialized)){
                return true;
            }else{
                metaStorage.delete(objectKey);
            }
        }
        return false;
    }

    /**
     * 删除对象元数据
     * @param objectKey objectKey
     * @return Long
     */
    public Long deleteMeta(String objectKey){
        cache.invalidate(objectKey);
        String objectId = getObjectId(objectKey);
        if(objectId != null && metaStorage.delete(objectId)){
            return Long.parseLong(objectId);
        }
        return null;
    }

    /**
     * 定位object的replica在哪些存储服务器上
     * @param objectId objectID
     * @return {@link Set} {@link String} 存储服务地址
     */
    public Set<String> locateObject(Long objectId){
        return objectLocations.get(objectId);
    }

    /**
     * 添加object副本位置
     * @param objectId objectId
     * @param location 副本地址
     */
    public void addObjectReplicaLocation(long objectId, String location){
        objectLocations.putIfAbsent(objectId, new HashSet<>());
        objectLocations.computeIfPresent(objectId, (k,v)->{
            v.add(location);
            return v;
        });
    }

    /**
     * 删除object副本位置
     * @param objectId objectID
     * @param location 副本地址
     */
    public void deleteObjectReplicaLocation(long objectId, String location){
        objectLocations.computeIfPresent(objectId, (k,v)->{
            v.remove(location);
            return v;
        });
    }

    /**
     * 获取object副本位置
     * @param id objectId
     * @return 副本地址集合
     */
    public Set<String> getObjectReplicaLocations(long id){
        return objectLocations.get(id);
    }

    public Set<Long> listObjectIds() {
        return objectLocations.keySet();
    }
}
