package com.jay.oss.proxy.http.handler;

import com.jay.oss.proxy.http.OssHttpRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;

import java.util.Map;

/**
 * <p>
 *  http handler
 * </p>
 *
 * @author Jay
 * @date 2022/01/25 14:41
 */
public interface HttpRequestHandler {
    /**
     * 处理http请求
     * @param context {@link ChannelHandlerContext}
     * @param request {@link FullHttpRequest}
     * @return {@link FullHttpResponse}
     */
    FullHttpResponse handle(ChannelHandlerContext context, OssHttpRequest request);
}
