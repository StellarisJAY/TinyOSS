package com.jay.oss.proxy.http;

import com.jay.dove.common.AbstractLifeCycle;
import com.jay.oss.common.entity.FilePart;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/01/25 11:26
 */
@Slf4j
public class HttpServer extends AbstractLifeCycle {
    private final EventLoopGroup boss = new NioEventLoopGroup(1);
    private final EventLoopGroup worker = new NioEventLoopGroup();

    private ServerBootstrap bootstrap;
    @Override
    public void startup() {
        long start = System.currentTimeMillis();
        super.startup();
        bootstrap = new ServerBootstrap()
                .group(boss, worker)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel channel) throws Exception {
                        ChannelPipeline pipeline = channel.pipeline();
                        pipeline.addLast(new HttpServerCodec());
                        pipeline.addLast(new HttpObjectAggregator(FilePart.DEFAULT_PART_SIZE + 32));
                    }
                });
        ChannelFuture future = null;
        try {
            future = bootstrap.bind(9000).sync();
            if(future.isSuccess()){
                log.info("proxy server started, time used: {}ms", (System.currentTimeMillis() - start));
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
