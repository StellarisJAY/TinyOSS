package com.jay.oss.common.util;

import com.jay.dove.transport.Url;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/03/07 15:43
 */
public class UrlUtil {
    public static List<Url> parseUrls(String str){
        String[] urls = str.split(";");
        List<Url> result = new ArrayList<>(urls.length);
        for (String url : urls) {
            result.add(Url.parseString(url));
        }
        return result;
    }

    public static String stringify(List<Url> urls){
        StringBuilder builder = new StringBuilder();
        for (Url url : urls) {
            builder.append(url.getOriginalUrl());
            builder.append(";");
        }
        return builder.toString();
    }
}
