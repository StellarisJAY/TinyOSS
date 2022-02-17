package com.jay.oss.storage.meta;

import com.jay.oss.common.entity.Bucket;
import com.jay.oss.common.entity.FileMeta;
import com.jay.oss.common.util.AppIdUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 *  桶管理器
 * </p>
 *
 * @author Jay
 * @date 2022/02/16 10:39
 */
@Slf4j
public class BucketManager {
    /**
     * 桶缓存
     * bucketKey: bucketName + appId
     */
    private final ConcurrentHashMap<String, Bucket> bucketCache = new ConcurrentHashMap<>(256);

    /**
     * 桶内元数据缓存
     */
    private final ConcurrentHashMap<String, List<FileMeta>> objectMetas = new ConcurrentHashMap<>(256);

    private final AtomicInteger appIdProvider = new AtomicInteger(0);

    /**
     * 添加bucket
     * @param bucket {@link Bucket}
     * @return appId + accessKey + secretKey
     */
    public String addBucket(Bucket bucket){
        // 生成AppId
        long appId = AppIdUtil.getAppId();
        bucket.setAppId(appId);
        String key = bucket.getBucketName() + "-" + appId;
        // 生成 ak和secret
        String accessKey = UUID.randomUUID().toString();
        String secretKey = UUID.randomUUID().toString();
        bucket.setAccessKey(accessKey);
        bucket.setSecretKey(secretKey);
        bucketCache.put(key, bucket);
        return appId + ";" + accessKey + ";" + secretKey;
    }

    public void saveBucket(Bucket bucket){
        bucketCache.put(bucket.getBucketName() + "-" + bucket.getAppId(), bucket);
    }

    public Bucket getBucket(String key){
        return bucketCache.get(key);
    }

    public void saveMeta(String key, FileMeta fileMeta){
        objectMetas.computeIfAbsent(key, k->{
            return new CopyOnWriteArrayList<>();
        });
        objectMetas.get(key).add(fileMeta);
    }

    /**
     * List bucket中一定范围的objects
     * @param key bucketKey
     * @param count 获取数量
     * @param offset 起始位置
     * @return {@link List<FileMeta>}
     */
    public List<FileMeta> listBucket(String key, int count, int offset){
        List<FileMeta> objects = objectMetas.get(key);
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

    public List<Bucket> snapshot(){
        return new ArrayList<>(bucketCache.values());
    }
}
