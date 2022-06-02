package com.jay.oss.proxy.http.handler;

import com.jay.oss.proxy.http.OssHttpRequest;
import com.jay.oss.proxy.util.HttpUtil;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.*;
import io.netty.util.internal.StringUtil;
import io.prometheus.client.Gauge;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
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

    /**
     * 存储桶名称路径，英文字母和数字组成，至少三个字符，第一个字符只能是英文字母。
     */
    private static final String BUCKET_OPT_PATTERN = "/[A-Z a-z][a-z A-Z 0-9][a-z A-Z 0-9][a-z A-Z 0-9]*";

    private static final String OBJECT_OPT_PATTERN = BUCKET_OPT_PATTERN + "-[0-9]*/[A-Z a-z][a-z A-Z 0-9][a-z A-Z 0-9][a-z A-Z 0-9 .]*";

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
                ctx.channel().writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
            }else{
                OssHttpRequest httpRequest = new OssHttpRequest(request);
                // 业务线程池处理请求
                Runnable runnable = ()->{
                    // 调用处理器方法
                    FullHttpResponse resp = handler.handle(ctx, httpRequest);
                    ctx.channel().writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE);
                };
                executor.execute(runnable);
            }

        }
    }

    private HttpRequestHandler selectHandler(FullHttpRequest request){
        String uri = request.uri();
        int idx = uri.indexOf("?");
        String path;
        if(idx != -1){
            path = uri.substring(0, idx);
        }else{
            path = uri;
        }
        if(path.matches(BUCKET_OPT_PATTERN)){
            return HandlerMapping.getHandler("bucket");
        }else if(path.matches(OBJECT_OPT_PATTERN)){
            return HandlerMapping.getHandler("object");
        }else{
            return null;
        }
    }
}
