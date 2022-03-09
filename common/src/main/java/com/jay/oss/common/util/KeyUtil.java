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
    public static String getObjectKey(String key, String bucket, String version){
        return bucket + "/" + key + (StringUtil.isNullOrEmpty(version) ? "" : "-" + version);
    }
}
