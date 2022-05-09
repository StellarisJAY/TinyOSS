package com.jay.oss.tracker.processor;

import com.jay.dove.transport.command.AbstractProcessor;
import com.jay.dove.transport.command.CommandCode;
import com.jay.oss.common.registry.StorageNodeInfo;
import com.jay.oss.common.registry.simple.SimpleRegistry;
import com.jay.oss.common.remoting.TinyOssCommand;
import com.jay.oss.common.remoting.TinyOssProtocol;
import com.jay.oss.common.util.SerializeUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 *  Tracker注册中心请求处理器
 * </p>
 *
 * @author Jay
 * @date 2022/05/09 10:18
 */
@Slf4j
public class SimpleRegistryProcessor extends AbstractProcessor {

    private final SimpleRegistry simpleRegistry;

    public SimpleRegistryProcessor(SimpleRegistry simpleRegistry) {
        this.simpleRegistry = simpleRegistry;
    }

    @Override
    public void process(ChannelHandlerContext channelHandlerContext, Object o) {
        if(o instanceof TinyOssCommand){
            TinyOssCommand command = (TinyOssCommand) o;
            CommandCode code = command.getCommandCode();
            if(code.equals(TinyOssProtocol.REGISTER_STORAGE)){
                processRegister(channelHandlerContext, command);
            }
            else if(code.equals(TinyOssProtocol.STORAGE_HEART_BEAT)){
                processHeartbeat(channelHandlerContext, command);
            }
        }
    }

    /**
     * 处理注册请求
     * @param context {@link ChannelHandlerContext}
     * @param command {@link TinyOssCommand}
     */
    private void processRegister(ChannelHandlerContext context, TinyOssCommand command){
        StorageNodeInfo storageNodeInfo = SerializeUtil.deserialize(command.getContent(), StorageNodeInfo.class);
        simpleRegistry.putStorageNode(storageNodeInfo);
        Attribute<String> attr = context.channel().attr(SimpleRegistry.STORAGE_NODE_ATTR);
        if(attr.get() == null){
            attr.set(storageNodeInfo.getUrl());
        }
        log.info("Storage node online: {}", storageNodeInfo.getUrl());
    }

    /**
     * 处理心跳
     * @param context {@link ChannelHandlerContext}
     * @param command {@link TinyOssCommand}
     */
    private void processHeartbeat(ChannelHandlerContext context, TinyOssCommand command){
        StorageNodeInfo storageNodeInfo = SerializeUtil.deserialize(command.getContent(), StorageNodeInfo.class);
        simpleRegistry.updateStorageNode(storageNodeInfo);
        Attribute<String> attr = context.channel().attr(SimpleRegistry.STORAGE_NODE_ATTR);
        if(attr.get() == null){
            attr.set(storageNodeInfo.getUrl());
        }
    }
}
