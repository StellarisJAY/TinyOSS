package com.jay.oss.tracker.meta;

import com.jay.oss.common.entity.Bucket;
import com.jay.oss.common.util.AppIdUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * <p>
 *  桶管理器，记录存储桶信息和存储桶内的对象信息
 * </p>
 *
 * @author Jay
 * @date 2022/02/21 10:09
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
    private final ConcurrentHashMap<String, List<String>> objectMetas = new ConcurrentHashMap<>(256);
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
        bucketCache.put(key, bucket);
        return bucket;
    }

    /**
     * 保存存储桶
     * @param bucket {@link Bucket}
     */
    public void saveBucket(Bucket bucket){
        bucketCache.put(bucket.getBucketName() + "-" + bucket.getAppId(), bucket);
    }

    /**
     * 获取存储桶
     * @param key key
     * @return {@link Bucket}
     */
    public Bucket getBucket(String key){
        return bucketCache.get(key);
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

    /**
     * 获取存储桶缓存快照
     * @return {@link List<Bucket>}
     */
    public List<Bucket> snapshot(){
        return new ArrayList<>(bucketCache.values());
    }
}
