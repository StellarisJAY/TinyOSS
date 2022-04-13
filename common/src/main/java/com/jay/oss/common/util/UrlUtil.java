package com.jay.oss.common.util;

import com.jay.dove.transport.Url;
import com.jay.oss.common.registry.StorageNodeInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 *  Url工具
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

    public static String stringifyFromNodes(List<StorageNodeInfo> nodes){
        // 拼接候选url
        StringBuilder builder = new StringBuilder();
        for (StorageNodeInfo node : nodes) {
            builder.append(node.getUrl());
            builder.append(";");
        }
        return builder.toString();
    }

    public static List<Url> parseUrls(String[] urls, int n){
        List<Url> result = new ArrayList<>(n);
        for (int i = 0; i < n && i < urls.length; i++) {
            result.add(Url.parseString(urls[i]));
        }
        return result;
    }
}
