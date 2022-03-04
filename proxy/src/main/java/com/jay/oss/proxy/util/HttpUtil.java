package com.jay.oss.proxy.util;

import com.alibaba.fastjson.JSON;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.util.StringUtil;
import com.jay.oss.proxy.entity.Result;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;

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

    public static FullHttpResponse createHttpResponse(HttpResponseStatus status, Result result){
        String json = JSON.toJSONString(result);
        ByteBuf content = Unpooled.wrappedBuffer(json.getBytes(OssConfigs.DEFAULT_CHARSET));
        DefaultFullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content);
        response.headers().set("Content-Type", "application/json");
        return response;
    }

    public static FullHttpResponse okResponse(Result result){
        return createHttpResponse(HttpResponseStatus.OK, result);
    }
    public static FullHttpResponse notFoundResponse(Result result){
        return createHttpResponse(HttpResponseStatus.NOT_FOUND, result);
    }
    public static FullHttpResponse internalErrorResponse(Result result){
        return createHttpResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, result);
    }
    public static FullHttpResponse forbiddenResponse(Result result){
        return createHttpResponse(HttpResponseStatus.FORBIDDEN, result);
    }

    public static FullHttpResponse createHttpResponse(HttpResponseStatus status, String message){
        Result result = new Result();
        result.setMessage(message);
        return createHttpResponse(status, result);
    }
    public static FullHttpResponse createHttpResponse(HttpResponseStatus status, ByteBuf content){
        return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content);
    }

    public static FullHttpResponse okResponse(){
        return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
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
