package com.jay.oss.common.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import java.nio.charset.StandardCharsets;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/02/21 14:54
 */
public class HttpUtil {
    public static FullHttpResponse createHttpResponse(HttpResponseStatus status, String message){
        if(!StringUtil.isNullOrEmpty(message)){
            ByteBuf content = Unpooled.wrappedBuffer(message.getBytes(StandardCharsets.UTF_8));
            return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content);
        }
        return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status);
    }
    public static FullHttpResponse createHttpResponse(HttpResponseStatus status, ByteBuf content){
        return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content);
    }

    public static FullHttpResponse okResponse(){
        return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    }

    public static FullHttpResponse okResponse(String message){
        return createHttpResponse(HttpResponseStatus.OK, message);
    }

    public static FullHttpResponse okResponse(ByteBuf content){
        return createHttpResponse(HttpResponseStatus.OK, content);
    }

    public static FullHttpResponse notFoundResponse(String message){
        return createHttpResponse(HttpResponseStatus.NOT_FOUND, message);
    }
    public static FullHttpResponse internalErrorResponse(String message){
        return createHttpResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, message);
    }

    public static FullHttpResponse unauthorizedResponse(String message){
        return createHttpResponse(HttpResponseStatus.UNAUTHORIZED, message);
    }

    public static FullHttpResponse forbiddenResponse(String message){
        return createHttpResponse(HttpResponseStatus.FORBIDDEN, message);
    }
}
