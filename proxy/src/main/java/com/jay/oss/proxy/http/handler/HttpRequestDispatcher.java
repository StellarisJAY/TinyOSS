package com.jay.oss.proxy.http.handler;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.*;
import io.netty.util.internal.StringUtil;
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
            // 获取handler
            HttpRequestHandler handler = selectHandler(request);
            FullHttpResponse response;
            if(handler == null){
                // handler不存在，BAD_REQUEST
                response = new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.BAD_REQUEST);
            }else{
                // 调用处理器方法
                response = handler.handle(ctx, request);
            }
            ctx.channel().writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }
    }

    private HttpRequestHandler selectHandler(FullHttpRequest request){
        String uri = request.uri();
        String path = uri.substring(uri.indexOf("/") + 1);
        HttpHeaders headers = request.headers();
        if(!StringUtil.isNullOrEmpty(path)){
            return HandlerMapping.getHandler("object");
        }
        return HandlerMapping.getHandler("bucket");
    }
}
