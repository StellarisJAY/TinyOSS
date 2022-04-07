package com.jay.oss.tracker.track;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.jay.oss.common.bitcask.BitCaskStorage;
import com.jay.oss.common.bitcask.Index;
import com.jay.oss.common.entity.MultipartUploadTask;
import com.jay.oss.common.util.SerializeUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/03/07 14:56
 */
@Slf4j
public class MultipartUploadTracker {
    /**
     * 热点数据缓存
     */
    private final Cache<String, MultipartUploadTask> cache = Caffeine.newBuilder()
            .maximumSize(1024)
            .expireAfterWrite(60, TimeUnit.SECONDS)
            .recordStats()
            .build();
    /**
     * 持久化存储
     */
    private final BitCaskStorage multipartTaskStorage;

    public MultipartUploadTracker(BitCaskStorage multipartTaskStorage) {
        this.multipartTaskStorage = multipartTaskStorage;
    }


    public MultipartUploadTask getTask(String uploadId){
        try{
            MultipartUploadTask task = cache.getIfPresent(uploadId);
            if(task == null){
                byte[] bytes = multipartTaskStorage.get(uploadId);
                if(bytes != null && bytes.length > 0){
                    task = SerializeUtil.deserialize(bytes, MultipartUploadTask.class);
                    cache.put(uploadId, task);
                }
            }
            return task;
        }catch (Exception e){
            log.error("Get Multipart Upload Task Failed, uploadId={}", uploadId, e);
            return null;
        }
    }

    public boolean saveUploadTask(String uploadId, MultipartUploadTask task){
        try{
            byte[] serialized = SerializeUtil.serialize(task, MultipartUploadTask.class);
            return multipartTaskStorage.put(uploadId, serialized);
        }catch (Exception e){
            log.error("Put Upload Task Failed, uploadId={}", uploadId, e);
            return false;
        }
    }

    public void remove(String uploadId){
        cache.invalidate(uploadId);
        multipartTaskStorage.delete(uploadId);
    }

    public void saveIndex(String uploadId, Index index){
        multipartTaskStorage.saveIndex(uploadId, index);
    }

    public Index getIndex(String uploadId){
        return multipartTaskStorage.getIndex(uploadId);
    }

    public List<String> listUploads(){
        return multipartTaskStorage.keys();
    }
}
