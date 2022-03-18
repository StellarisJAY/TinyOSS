package com.jay.oss.tracker.track;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.bitcask.BitCaskStorage;
import com.jay.oss.common.bitcask.Index;
import com.jay.oss.common.entity.object.ObjectMeta;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.common.util.StringUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
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
     * cache table
     * 热点数据缓存
     */
    private final ConcurrentHashMap<String, String> locationCache = new ConcurrentHashMap<>();

    /**
     * 热点元数据缓存
     */
    private final Cache<String, ObjectMeta> cache = CacheBuilder.newBuilder()
            .expireAfterAccess(60, TimeUnit.MINUTES)
            .concurrencyLevel(10)
            .maximumSize(100000)
            .recordStats().build();
    /**
     * 元数据磁盘存储
     */
    private final BitCaskStorage metaStorage = new BitCaskStorage("meta");

    /**
     * 初始化object tracker
     * 扫描chunk路径，获取已经存在的chunk文件
     * @throws Exception e
     */
    public void init() throws Exception {
        metaStorage.init();
    }

    /**
     * 定位object在哪些主机上
     * @param objectKey objectKey
     * @return Url String
     */
    public String locateObject(String objectKey){
        ObjectMeta objectMeta = getObjectMeta(objectKey);
        return objectMeta == null ? null : objectMeta.getLocations();
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
        }catch (IOException e){
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
        try{
            byte[] serialized = SerializeUtil.serialize(meta, ObjectMeta.class);
            return metaStorage.put(objectKey, serialized);
        }catch (IOException e){
            log.error("Write Object Meta Failed, meta: {}", meta.getObjectKey());
            return false;
        }
    }

    /**
     * 定位并删除object
     * @param objectKey objectKey
     * @return Url String
     */
    public String locateAndDeleteObject(String objectKey){
        String result = locateObject(objectKey);
        deleteObject(objectKey);
        return result;
    }

    /**
     * 获取object的索引
     * @param key objectKey
     * @return {@link Index}
     */
    public Index getIndex(String key){
        return metaStorage.getIndex(key);
    }

    /**
     * 保存object索引
     * @param key objectKey
     * @param index {@link Index}
     */
    public void saveObjectIndex(String key, Index index){
        metaStorage.saveIndex(key, index);
    }

    public void deleteObject(String key){
        locationCache.remove(key);
        metaStorage.delete(key);
    }

    /**
     * BitCask存储模型merge
     */
    public void merge() throws Exception {
        metaStorage.merge();
        metaStorage.completeMerge();
    }

    public List<Index> listIndexes(){
        return metaStorage.listIndex();
    }
}
