package com.jay.oss.proxy.http;

import com.jay.oss.proxy.constant.HttpConstants;
import com.jay.oss.proxy.util.HttpUtil;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

import java.util.Map;

/**
 * <p>
 *  FastOss Http 请求封装
 * </p>
 *
 * @author Jay
 * @date 2022/03/11 11:15
 */
public class OssHttpRequest {
    private final FullHttpRequest originalRequest;
    private final Map<String, String> parameters;
    private final String path;
    private final HttpHeaders headers;

    public OssHttpRequest(FullHttpRequest originalRequest) {
        this.originalRequest = originalRequest;
        String uri = originalRequest.uri();
        this.parameters = HttpUtil.parseUri(uri);
        int i = uri.indexOf("?");
        if(i == -1){
            path = uri.substring(1);
        }else{
            path = uri.substring(1, i);
        }
        this.headers = originalRequest.headers();
    }

    public String host(){
        return headers.get(HttpConstants.HOST);
    }
    public String contentType(){
        return headers.get(HttpConstants.CONTENT_TYPE);
    }
    public int contentLength(){
        return Integer.parseInt(headers.get(HttpConstants.CONTENT_LENGTH));
    }
    public String contentMd5(){
        return headers.get(HttpConstants.CONTENT_MD5);
    }
    public String range(){
        return headers.get(HttpConstants.RANGE);
    }
    public String authorization(){
        return headers.get(HttpConstants.AUTHORIZATION);
    }
    public String acl(){
        return headers.get(HttpConstants.ACL);
    }

    public String getParameter(String name){
        return parameters.get(name);
    }
    public boolean containsParameter(String name){
        return parameters.containsKey(name);
    }
    public FullHttpRequest getOriginalRequest(){
        return originalRequest;
    }
    public String getPath(){
        return path;
    }
    public HttpMethod getMethod(){
        return originalRequest.method();
    }
    public HttpVersion protocolVersion(){
        return originalRequest.protocolVersion();
    }
    public ByteBuf content(){
        return originalRequest.content();
    }

    public String getBucket(){
        return headers.get("Bucket");
    }
}
