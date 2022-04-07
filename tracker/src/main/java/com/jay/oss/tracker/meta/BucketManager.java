package com.jay.oss.tracker.meta;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.jay.oss.common.bitcask.BitCaskStorage;
import com.jay.oss.common.bitcask.Index;
import com.jay.oss.common.entity.bucket.Bucket;
import com.jay.oss.common.util.AppIdUtil;
import com.jay.oss.common.util.SerializeUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  桶管理器，记录存储桶信息和存储桶内的对象信息
 * </p>
 *
 * @author Jay
 * @date 2022/02/21 10:09
 */
@Slf4j
public class BucketManager {
    /**
     * 桶内元数据缓存
     */
    private final ConcurrentHashMap<String, List<String>> objectMetas = new ConcurrentHashMap<>(256);

    /**
     * 桶磁盘存储
     */
    private final BitCaskStorage bucketStorage;
    /**
     * 存储桶LRU缓存
     */
    private final Cache<String, Bucket> cache = CacheBuilder.newBuilder()
            .maximumSize(2048).concurrencyLevel(10)
            .expireAfterAccess(60, TimeUnit.MINUTES)
            .recordStats()
            .build();

    public BucketManager(BitCaskStorage bucketStorage) {
        this.bucketStorage = bucketStorage;
    }

    /**
     * 添加bucket
     * @param bucket {@link Bucket}
     * @return appId + accessKey + secretKey
     */
    public Bucket addBucket(Bucket bucket){
        // 生成AppId
        long appId = AppIdUtil.getAppId();
        bucket.setAppId(appId);
        String key = bucket.getBucketName() + "-" + appId;
        // 生成 ak和secret
        String accessKey = UUID.randomUUID().toString();
        String secretKey = UUID.randomUUID().toString();
        bucket.setAccessKey(accessKey);
        bucket.setSecretKey(secretKey);

        try{
            byte[] serialized = SerializeUtil.serialize(bucket, Bucket.class);
            boolean status = bucketStorage.put(key, serialized);
            return bucket;
        }catch (Exception e){
            log.error("Put Bucket Failed, key: {}", key, e);
            return null;
        }
    }

    /**
     * 更新存储桶元数据
     * @param bucketKey bucket KEY
     * @param bucket {@link Bucket}
     * @return boolean
     */
    public boolean updateBucket(String bucketKey, Bucket bucket){
        try{
            cache.invalidate(bucketKey);
            byte[] serialized = SerializeUtil.serialize(bucket, Bucket.class);
            return bucketStorage.update(bucketKey, serialized);
        }catch (IOException e){
            log.warn("Update Bucket Failed, bucket: {} ", bucketKey, e);
            return false;
        }
    }

    /**
     * 获取存储桶
     * @param key key
     * @return {@link Bucket}
     */
    public Bucket getBucket(String key){
        try{
            Bucket bucket = cache.getIfPresent(key);
            if(bucket == null){
                byte[] serialized = bucketStorage.get(key);
                if(serialized != null && serialized.length > 0){
                    bucket = SerializeUtil.deserialize(serialized, Bucket.class);
                    cache.put(key, bucket);
                    return bucket;
                }
            }else{
                return bucket;
            }
        }catch (IOException e){
            log.error("Get Bucket Failed, key: {}", key, e);
        }
        return null;
    }

    /**
     * 保存object记录
     * @param bucket bucket Name
     * @param objectKey objectKey
     */
    public void putObject(String bucket, String objectKey){
        // 创建object列表，concurrentHashMap的computeIfAbsent保证线程安全
        objectMetas.computeIfAbsent(bucket, k-> new CopyOnWriteArrayList<>());
        // copyOnWrite 添加记录
        objectMetas.get(bucket).add(objectKey);
    }

    /**
     * List bucket中一定范围的objects
     * @param bucket bucket Name
     * @param count 获取数量
     * @param offset 起始位置
     * @return {@link List<String>}
     */
    public List<String> listBucket(String bucket, int count, int offset){
        List<String> objects = objectMetas.get(bucket);
        // offset 超出objects范围
        if(objects == null || offset >= objects.size()){
            // 返回空
            return new ArrayList<>();
        }
        // count 超出 objects范围
        if(count >= objects.size()){
            // 返回offset到objects最后一个
            return objects.subList(offset, objects.size());
        }else{
            return objects.subList(offset, offset + count);
        }
    }

    /**
     * 删除存储桶中的object记录
     * @param bucket 桶
     * @param objectKey object key
     * @return boolean 删除是否成功
     */
    public boolean deleteObject(String bucket, String objectKey){
        // 获取list
        List<String> objects = objectMetas.get(bucket);
        if(objects != null && !objects.isEmpty()){
            // 按条件删除，该list是copyOnWriteList，保证了线程安全
            return objects.remove(objectKey);
        }
        return false;
    }

    public Index getIndex(String key){
        return bucketStorage.getIndex(key);
    }

    public void saveIndex(String key, Index index){
        bucketStorage.saveIndex(key, index);
    }


    public List<String> listBuckets(){
        return bucketStorage.keys();
    }
}
