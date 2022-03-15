package com.jay.oss.common.util;

/**
 * <p>
 *  ObjectKey工具
 * </p>
 *
 * @author Jay
 * @date 2022/03/09 15:57
 */
public class KeyUtil {
    /**
     * 规定的objectKey格式：Bucket/{key}-version
     * @param key key 上传时的fileName
     * @param bucket bucket
     * @param version version
     * @return ObjectKey
     */
    public static String getObjectKey(String key, String bucket, String version){
        return bucket + "/" + key + (StringUtil.isNullOrEmpty(version) ? "" : "/" + version);
    }

    public static String[] split(String objectKey){
        return objectKey.split("/");
    }

    public static String getBucket(String objectKey){
        return split(objectKey)[0];
    }
}
