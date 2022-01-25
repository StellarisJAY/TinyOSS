package com.jay.oss.proxy.http.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpRequest;

/**
 * <p>
 *  Http请求分发器
 * </p>
 *
 * @author Jay
 * @date 2022/01/25 11:33
 */
public class HttpRequestDispatcher extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if(msg instanceof DefaultFullHttpRequest){
            DefaultFullHttpRequest request = (DefaultFullHttpRequest) msg;

        }
    }
}
