package com.jay.oss.common.util;

import com.jay.oss.common.config.OssConfigs;

import java.nio.charset.StandardCharsets;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/02/21 14:43
 */
public class StringUtil {
    public static boolean isNullOrEmpty(String s){
        return s == null || s.length() == 0;
    }

    public static String toString(byte[] bytes){
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static byte[] getBytes(String s){
        return s.getBytes(OssConfigs.DEFAULT_CHARSET);
    }
}
