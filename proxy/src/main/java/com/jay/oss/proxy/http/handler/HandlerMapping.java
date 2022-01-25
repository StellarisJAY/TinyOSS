package com.jay.oss.proxy.http.handler;

import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 *  HandlerMapping
 * </p>
 *
 * @author Jay
 * @date 2022/01/25 14:42
 */
public class HandlerMapping {
    private static final ConcurrentHashMap<String, HttpRequestHandler> HANDLER_MAP = new ConcurrentHashMap<>(256);

    public static HttpRequestHandler getHandler(String path){
        return HANDLER_MAP.get(path);
    }

    public static void registerHandler(String path, HttpRequestHandler handler){
        HANDLER_MAP.putIfAbsent(path, handler);
    }
}
