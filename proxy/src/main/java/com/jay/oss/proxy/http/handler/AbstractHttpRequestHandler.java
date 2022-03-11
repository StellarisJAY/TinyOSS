package com.jay.oss.proxy.http.handler;

import com.jay.oss.proxy.http.OssHttpRequest;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * <p>
 *  HttpHandler抽象类
 *  提供主要的五种HTTP方法的实现
 * </p>
 *
 * @author Jay
 * @date 2022/01/25 19:51
 */
@Slf4j
public abstract class AbstractHttpRequestHandler implements HttpRequestHandler{

    public FullHttpResponse handleGet(ChannelHandlerContext context, OssHttpRequest request) throws Exception{
        return new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.METHOD_NOT_ALLOWED);
    }

    public FullHttpResponse handlePost(ChannelHandlerContext context, OssHttpRequest request) throws Exception{
        return new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.METHOD_NOT_ALLOWED);
    }

    public FullHttpResponse handlePut(ChannelHandlerContext context, OssHttpRequest request) throws Exception {
        return new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.METHOD_NOT_ALLOWED);
    }

    public FullHttpResponse handleDelete(ChannelHandlerContext context, OssHttpRequest request) throws Exception{
        return new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.METHOD_NOT_ALLOWED);
    }
    @Override
    public final FullHttpResponse handle(ChannelHandlerContext context, OssHttpRequest request) {
        HttpMethod method = request.getMethod();
        try{
            if (HttpMethod.GET.equals(method)) {
                return handleGet(context, request);
            } else if (HttpMethod.POST.equals(method)) {
                return handlePost(context, request);
            } else if (HttpMethod.PUT.equals(method)) {
                return handlePut(context, request);
            } else if (HttpMethod.DELETE.equals(method)) {
                return handleDelete(context, request);
            } else if (HttpMethod.OPTIONS.equals(method)) {
                return handleOptions(context, request);
            }
            return new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.METHOD_NOT_ALLOWED);
        }catch (Exception e){
            log.error("request handler error: ", e);
            return new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }finally {
            // 释放content，避免堆外内存OOM
            ByteBuf content = request.content();
            int refCnt = content.refCnt();
            if(refCnt > 0){
                content.release(refCnt);
            }
        }

    }

    public FullHttpResponse handleOptions(ChannelHandlerContext context, OssHttpRequest request){
        DefaultFullHttpResponse response = new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.OK);
        HttpHeaders headers = response.headers();
        headers.set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS, "POST, GET, OPTIONS, PUT, DELETE");
        return response;
    }
}
