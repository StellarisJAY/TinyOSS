package com.jay.oss.storage.meta;

import com.jay.oss.common.entity.Bucket;
import com.jay.oss.common.entity.FileMeta;
import com.jay.oss.common.util.AppIdUtil;

import java.util.List;
import java.util.Set;
import java.util.UUID;
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

    public Bucket getBucket(String key){
        return bucketCache.get(key);
    }

    public void saveMeta(String key, FileMeta fileMeta){
        objectMetas.computeIfAbsent(key, k->{
            return new CopyOnWriteArrayList<>();
        });
        objectMetas.get(key).add(fileMeta);
    }

    public List<FileMeta> listBucket(String key){
        return objectMetas.get(key);
    }
}
