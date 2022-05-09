package com.jay.oss.tracker.processor;

import com.jay.dove.transport.command.AbstractProcessor;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.entity.response.StorageHeartBeatResponse;
import com.jay.oss.common.registry.StorageNodeInfo;
import com.jay.oss.common.registry.simple.SimpleRegistry;
import com.jay.oss.common.remoting.TinyOssCommand;
import com.jay.oss.common.remoting.TinyOssProtocol;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.common.entity.task.DeleteTask;
import com.jay.oss.common.entity.task.ReplicaTask;
import com.jay.oss.tracker.task.StorageTaskManager;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

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
    private final StorageTaskManager storageTaskManager;
    private final CommandFactory commandFactory;

    public SimpleRegistryProcessor(SimpleRegistry simpleRegistry, StorageTaskManager taskManager, CommandFactory commandFactory) {
        this.simpleRegistry = simpleRegistry;
        this.commandFactory = commandFactory;
        this.storageTaskManager = taskManager;
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
        RemotingCommand response = commandFactory.createResponse(command.getId(), "", TinyOssProtocol.SUCCESS);
        sendResponse(context, response);
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
        List<ReplicaTask> replicaTasks = storageTaskManager.pollReplicaTasks(storageNodeInfo.getUrl(), 100);
        List<DeleteTask> deleteTasks = storageTaskManager.pollDeleteTask(storageNodeInfo.getUrl(), 100);
        StorageHeartBeatResponse heartBeatResponse = new StorageHeartBeatResponse(replicaTasks, deleteTasks);
        byte[] content = SerializeUtil.serialize(heartBeatResponse, StorageHeartBeatResponse.class);
        RemotingCommand response = commandFactory.createResponse(command.getId(), content, TinyOssProtocol.SUCCESS);
        sendResponse(context, response);
    }
}
