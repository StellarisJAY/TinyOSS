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
    private static final ConcurrentHashMap<HttpRequestInfo, HttpRequestHandler> HANDLER_MAP = new ConcurrentHashMap<>(256);

    public static HttpRequestHandler getHandler(HttpRequestInfo requestInfo){
        return HANDLER_MAP.get(requestInfo);
    }

    public static void registerHandler(HttpRequestInfo requestInfo, HttpRequestHandler handler){
        HANDLER_MAP.putIfAbsent(requestInfo, handler);
    }
}
