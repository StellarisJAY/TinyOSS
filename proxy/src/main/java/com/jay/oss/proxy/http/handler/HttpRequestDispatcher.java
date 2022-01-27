package com.jay.oss.proxy.http.handler;

import io.netty.buffer.Unpooled;
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
            // 从handlerMapping找到handler
            HttpRequestHandler handler = HandlerMapping.getHandler(request.uri());
            if(handler == null){
                // handler不存在，BAD_REQUEST
                FullHttpResponse response = new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.BAD_REQUEST);
                ctx.channel().writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
            }else{
                // executor 处理请求
                executor.submit(()->{
                    try{
                        // 调用处理器方法
                        FullHttpResponse response = handler.handle(ctx, request);
                        ctx.channel().writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
                    }catch (Exception e){
                        log.warn("handler execution error, ", e);
                        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
                        ctx.channel().writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
                    }
                });
            }
        }
    }
}
