package com.jay.oss.tracker.track;

import com.jay.oss.common.entity.MultipartUploadTask;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/03/07 14:56
 */
public class MultipartUploadTracker {
    private final ConcurrentHashMap<String, MultipartUploadTask> uploads = new ConcurrentHashMap<>();

    public String getUploadLocation(String uploadId){
        MultipartUploadTask task = uploads.get(uploadId);
        if(task != null){
            return task.getLocations();
        }else{
            return null;
        }
    }

    public MultipartUploadTask getTask(String uploadId){
        return uploads.get(uploadId);
    }

    public boolean saveUploadTask(String uploadId, MultipartUploadTask task){
        return uploads.putIfAbsent(uploadId, task) == null;
    }

    public List<MultipartUploadTask> listUploadTasks(){
        return new ArrayList<>(uploads.values());
    }

    public void remove(String uploadId){
        uploads.remove(uploadId);
    }
}
