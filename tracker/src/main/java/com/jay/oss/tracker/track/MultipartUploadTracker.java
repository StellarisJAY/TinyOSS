package com.jay.oss.tracker.track;

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
    private final ConcurrentHashMap<String, String> uploads = new ConcurrentHashMap<>();

    public String getUploadLocation(String uploadId){
        return uploads.get(uploadId);
    }

    public void saveUploadLocation(String uploadId, String locations){
        uploads.put(uploadId, locations);
    }
}
