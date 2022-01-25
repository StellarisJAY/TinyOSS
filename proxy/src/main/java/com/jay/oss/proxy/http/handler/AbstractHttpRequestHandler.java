package com.jay.oss.proxy.http.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;

/**
 * <p>
 *  HttpHandler抽象类
 *  提供主要的五种HTTP方法的实现
 * </p>
 *
 * @author Jay
 * @date 2022/01/25 19:51
 */
public abstract class AbstractHttpRequestHandler implements HttpRequestHandler{

    public FullHttpResponse handleGet(ChannelHandlerContext context, FullHttpRequest request){
        return new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.METHOD_NOT_ALLOWED);
    }

    public FullHttpResponse handlePost(ChannelHandlerContext context, FullHttpRequest request){
        return new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.METHOD_NOT_ALLOWED);
    }

    public FullHttpResponse handlePut(ChannelHandlerContext context, FullHttpRequest request){
        return new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.METHOD_NOT_ALLOWED);
    }

    public FullHttpResponse handleDelete(ChannelHandlerContext context, FullHttpRequest request){
        return new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.METHOD_NOT_ALLOWED);
    }
    @Override
    public final FullHttpResponse handle(ChannelHandlerContext context, FullHttpRequest request) {
        HttpMethod method = request.method();
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
    }

    public FullHttpResponse handleOptions(ChannelHandlerContext context, FullHttpRequest request){
        DefaultFullHttpResponse response = new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.OK);
        HttpHeaders headers = response.headers();
        headers.set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS, "POST, GET, OPTIONS, PUT, DELETE");
        return response;
    }
}
