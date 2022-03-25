package com.jay.oss.proxy.http;

import com.jay.dove.common.AbstractLifeCycle;
import com.jay.dove.util.NamedThreadFactory;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.proxy.http.handler.HttpRequestDispatcher;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  ProxyNode HTTP Server
 * </p>
 *
 * @author Jay
 * @date 2022/01/25 11:26
 */
@Slf4j
public class HttpServer extends AbstractLifeCycle {
    private final EventLoopGroup boss = new NioEventLoopGroup(1);
    private final EventLoopGroup worker = new NioEventLoopGroup();
    /**
     * 请求处理线程池，避免使用IO线程处理请求
     */
    private final ExecutorService handlerExecutor =
            new ThreadPoolExecutor(2 * Runtime.getRuntime().availableProcessors(),
            2 * Runtime.getRuntime().availableProcessors(),
            0, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(),
                    new NamedThreadFactory("handler-executor", true));
    private ServerBootstrap bootstrap;
    @Override
    public void startup() {
        long start = System.currentTimeMillis();
        super.startup();
        bootstrap = new ServerBootstrap()
                .group(boss, worker)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 64)
                .childHandler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel channel) throws Exception {
                        ChannelPipeline pipeline = channel.pipeline();
                        // Http codec
                        pipeline.addLast(new HttpServerCodec());
                        pipeline.addLast(new HttpObjectAggregator(OssConfigs.maxRequestBodySize()));
                        // 请求分发器
                        pipeline.addLast(new HttpRequestDispatcher(handlerExecutor));
                    }
                });
        ChannelFuture future = null;
        try {
            future = bootstrap.bind(9000).sync();
            if(future.isSuccess()){
                log.info("http server started, time used: {}ms", (System.currentTimeMillis() - start));
            }
        } catch (InterruptedException e) {
            log.error("proxy server failed to start, error: ", e);
            boss.shutdownGracefully();
            worker.shutdownGracefully();
        }

    }

    @Override
    public void shutdown() {
        super.shutdown();
        boss.shutdownGracefully();
        worker.shutdownGracefully();
    }
}
