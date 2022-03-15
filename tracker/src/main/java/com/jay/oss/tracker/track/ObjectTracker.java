package com.jay.oss.tracker.track;

import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.bitcask.BitCaskStorage;
import com.jay.oss.common.bitcask.Index;
import com.jay.oss.common.util.StringUtil;
import lombok.extern.slf4j.Slf4j;

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
     * BitCask存储模型
     */
    private final BitCaskStorage bitCaskStorage = new BitCaskStorage("location");

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
        try{
            // 从缓存获取
            String urls = locationCache.get(objectKey);
            if(urls == null){
                // 缓存未命中，从BitCask获取
                byte[] value = bitCaskStorage.get(objectKey);
                if(value != null){
                    String result = StringUtil.toString(value);
                    // 写入缓存
                    locationCache.putIfAbsent(objectKey, result);
                    return result;
                }
                else{
                    return null;
                }
            }
            return urls;
        }catch (Exception e){
            log.warn("locate object failed, key: {}, ", objectKey, e);
            return null;
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
        return bitCaskStorage.getIndex(key);
    }

    /**
     * 保存object索引
     * @param key objectKey
     * @param index {@link Index}
     */
    public void saveObjectIndex(String key, Index index){
        bitCaskStorage.saveIndex(key, index);
    }

    public void deleteObject(String key){
        locationCache.remove(key);
        bitCaskStorage.delete(key);
    }

    /**
     * BitCask存储模型merge
     */
    public void merge() throws Exception {
        bitCaskStorage.merge();
        bitCaskStorage.completeMerge();
    }

    public List<Index> listIndexes(){
        return bitCaskStorage.listIndex();
    }
}
