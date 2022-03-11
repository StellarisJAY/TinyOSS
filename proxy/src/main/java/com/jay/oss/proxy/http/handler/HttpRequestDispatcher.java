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

    private static final Gauge totalInProgress = Gauge.build()
            .name("total_requests")
            .help("total in-progress request count").register();

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
            totalInProgress.inc();
            FullHttpRequest request = (FullHttpRequest) msg;
            // 获取handler
            HttpRequestHandler handler = selectHandler(request);
            FullHttpResponse response;
            if(handler == null){
                // handler不存在，BAD_REQUEST
                response = new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.BAD_REQUEST);
            }else{
                OssHttpRequest httpRequest = new OssHttpRequest(request);
                // 调用处理器方法
                response = handler.handle(ctx, httpRequest);
            }
            ctx.channel().writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
            totalInProgress.dec();
        }
    }

    private HttpRequestHandler selectHandler(FullHttpRequest request){
        String uri = request.uri();
        int idx = uri.indexOf("?");
        String path;
        if(idx != -1){
            path = uri.substring(1, idx);
        }else{
            path = uri.substring(1);
        }
        HttpHeaders headers = request.headers();
        if(!StringUtil.isNullOrEmpty(path)){
            return HandlerMapping.getHandler("object");
        }
        return HandlerMapping.getHandler("bucket");
    }
}
