package com.jay.oss.proxy.http.handler;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.*;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;

/**
 * <p>
 *  Http请求分发器
 * </p>
 *
 * @author Jay
 * @date 2022/01/25 11:33
 */
@Slf4j
public class HttpRequestDispatcher extends ChannelInboundHandlerAdapter {
    /**
     * request 处理线程池
     */
    private final ExecutorService executor;
    public HttpRequestDispatcher(ExecutorService executor) {
        this.executor = executor;
    }
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if(msg instanceof FullHttpRequest){
            FullHttpRequest request = (FullHttpRequest) msg;
            HttpRequestInfo requestInfo = new HttpRequestInfo(request.uri(), request.method());
            // 从handlerMapping找到handler
            HttpRequestHandler handler = HandlerMapping.getHandler(requestInfo);
            // executor 处理请求
            executor.submit(()->{
                handler.handle(ctx, request);
            });
            // test responses, to be removed
            FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
            ctx.channel().writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }
    }
}
