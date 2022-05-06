package com.jay.oss.tracker.meta;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.jay.oss.common.entity.bucket.Bucket;
import com.jay.oss.common.kv.KvStorage;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.common.util.SnowflakeIdGenerator;
import lombok.extern.slf4j.Slf4j;

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
    private final KvStorage bucketStorage;

    private final SnowflakeIdGenerator idGenerator = new SnowflakeIdGenerator(0L, 0L);
    /**
     * 存储桶LRU缓存
     */
    private final Cache<String, Bucket> cache = Caffeine.newBuilder()
            .maximumSize(2048)
            .expireAfterAccess(60, TimeUnit.MINUTES)
            .recordStats()
            .build();

    public BucketManager(KvStorage bucketStorage) {
        this.bucketStorage = bucketStorage;
    }

    /**
     * 添加bucket
     * @param bucket {@link Bucket}
     * @return appId + accessKey + secretKey
     */
    public Bucket addBucket(Bucket bucket){
        // 生成AppId
        long appId = idGenerator.nextId();
        bucket.setAppId(appId);
        String key = bucket.getBucketName() + "-" + appId;
        // 生成 ak和secret
        String accessKey = UUID.randomUUID().toString();
        String secretKey = UUID.randomUUID().toString();
        bucket.setAccessKey(accessKey);
        bucket.setSecretKey(secretKey);

        try{
            byte[] serialized = SerializeUtil.serialize(bucket, Bucket.class);
            if(bucketStorage.putIfAbsent(key, serialized)){
                return bucket;
            }
            return null;
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
            bucketStorage.put(bucketKey, serialized);
            return true;
        }catch (Exception e){
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
        }catch (Exception e){
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

    public List<String> listBuckets(){
        return bucketStorage.keys();
    }
}
