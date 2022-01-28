package com.jay.oss.common.remoting;

import com.jay.dove.transport.HeartBeatTrigger;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 *  心跳trigger
 * </p>
 *
 * @author Jay
 * @date 2022/01/27 14:28
 */
@Slf4j
public class FastOssHeartBeatTrigger implements HeartBeatTrigger {
    @Override
    public void heartBeatTriggered(ChannelHandlerContext channelHandlerContext) {
        if(log.isDebugEnabled()){
            log.debug("heart-beat triggered, remote address: {}", channelHandlerContext.channel().remoteAddress());
        }
    }
}
