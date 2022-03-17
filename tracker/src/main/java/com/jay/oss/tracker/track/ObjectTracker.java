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
    private final Cache<String, Object> cache = CacheBuilder.newBuilder()
            .expireAfterAccess(60, TimeUnit.MINUTES)
            .concurrencyLevel(10)
            .maximumSize(100000)
            .recordStats().build();
    /**
     * BitCask存储模型
     */
    private final BitCaskStorage bitCaskStorage = new BitCaskStorage("location");

    private final BitCaskStorage metaStorage = new BitCaskStorage("meta");

    /**
     * 初始化object tracker
     * 扫描chunk路径，获取已经存在的chunk文件
     * @throws Exception e
     */
    public void init() throws Exception {
        bitCaskStorage.init();
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
            Object object = cache.getIfPresent(objectKey);
            if(object instanceof ObjectMeta){
                return (ObjectMeta) object;
            }else{
                byte[] serialized = metaStorage.get(objectKey);
                if(serialized != null && serialized.length > 0){
                    return SerializeUtil.deserialize(serialized, ObjectMeta.class);
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
    public boolean putObjectMeta(ObjectMeta meta)  {
        try{
            byte[] serialized = SerializeUtil.serialize(meta, ObjectMeta.class);
            log.info("Object Key : {}", meta.getObjectKey());
            return metaStorage.put(meta.getObjectKey(), serialized);
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
     * 保存object位置
     * @param objectKey objectKey
     * @param urls 位置urls
     * @return boolean
     */
    public boolean saveObjectLocation(String objectKey, String urls){
        try{
            byte[] urlBytes = urls.getBytes(OssConfigs.DEFAULT_CHARSET);
            return bitCaskStorage.put(objectKey, urlBytes);
        }catch (Exception e){
            log.error("Failed to save object location ", e);
            return false;
        }
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
