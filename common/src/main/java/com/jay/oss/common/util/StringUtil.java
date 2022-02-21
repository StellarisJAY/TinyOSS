package com.jay.oss.common.util;

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
}
