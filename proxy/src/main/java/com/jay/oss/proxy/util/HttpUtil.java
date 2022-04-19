package com.jay.oss.proxy.util;

import com.alibaba.fastjson.JSON;
import com.jay.dove.transport.command.CommandCode;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.proxy.entity.Result;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import java.util.HashMap;
import java.util.Map;

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

    public static FullHttpResponse okResponse(String message){
        return createHttpResponse(HttpResponseStatus.OK, message);
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

    public static FullHttpResponse badRequestResponse(String message){
        return createHttpResponse(HttpResponseStatus.BAD_REQUEST, message);
    }

    public static FullHttpResponse badRequestResponse(){
        return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST);
    }

    public static FullHttpResponse partialContentResponse(ByteBuf content){
        return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.PARTIAL_CONTENT, content);
    }

    public static FullHttpResponse bucketAclResponse(CommandCode code){
        if(FastOssProtocol.NOT_FOUND.equals(code)){
            return notFoundResponse("Bucket Not Found");
        }else if(FastOssProtocol.ACCESS_DENIED.equals(code)){
            return unauthorizedResponse("Bucket Access Denied");
        }else if(FastOssProtocol.OBJECT_NOT_FOUND.equals(code)){
            return notFoundResponse("Object Not Found");
        }else if(FastOssProtocol.DUPLICATE_OBJECT_KEY.equals(code)){
            return badRequestResponse("Duplicate Object key");
        }else if(FastOssProtocol.MULTIPART_UPLOAD_FINISHED.equals(code)){
            return badRequestResponse("Multipart Upload Already Finished");
        }
        else{
            return internalErrorResponse("Internal Server Error");
        }
    }

    public static FullHttpResponse errorResponse(CommandCode code){
        if(FastOssProtocol.NOT_FOUND.equals(code)){
            return notFoundResponse("Bucket Not Found");
        }else if(FastOssProtocol.ACCESS_DENIED.equals(code)){
            return unauthorizedResponse("Bucket Access Denied");
        }else if(FastOssProtocol.OBJECT_NOT_FOUND.equals(code)){
            return notFoundResponse("Object Not Found");
        }else if(FastOssProtocol.MULTIPART_UPLOAD_FINISHED.equals(code)){
            return badRequestResponse("Multipart Upload Already Finished");
        } else if(FastOssProtocol.DUPLICATE_OBJECT_KEY.equals(code)){
            return badRequestResponse("Duplicate Object key");
        }else{
            return internalErrorResponse("Internal Server Error");
        }
    }

    public static Map<String, String> parseUri(String uri){
        Map<String, String> result = new HashMap<>();
        int i = uri.indexOf("?");
        if(i != -1){
            String substring = uri.substring(i + 1);
            String[] parameters = substring.split("&");
            for (String parameter : parameters) {
                int idx = parameter.indexOf("=");
                if(idx == -1){
                    result.put(parameter, "");
                }else{
                    result.put(parameter.substring(0, idx), parameter.substring(idx + 1));
                }
            }
        }
        return result;
    }

    public static FullHttpResponse httpResponseOfCode(CommandCode code){
        if(FastOssProtocol.SUCCESS.equals(code)){
            return okResponse("Success");
        } else if(FastOssProtocol.NOT_FOUND.equals(code)){
            return notFoundResponse("Bucket Not Found");
        }else if(FastOssProtocol.ACCESS_DENIED.equals(code)){
            return unauthorizedResponse("Bucket Access Denied");
        }else if(FastOssProtocol.OBJECT_NOT_FOUND.equals(code)){
            return notFoundResponse("Object Not Found");
        }else if(FastOssProtocol.DUPLICATE_OBJECT_KEY.equals(code)){
            return badRequestResponse("Duplicate Object key");
        }else if(FastOssProtocol.MULTIPART_UPLOAD_FINISHED.equals(code)){
            return badRequestResponse("Multipart Upload Already Finished");
        }
        else{
            return internalErrorResponse("Internal Server Error");
        }
    }
}
