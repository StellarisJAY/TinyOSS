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
import com.jay.oss.tracker.track.ObjectTracker;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.stream.Collectors;

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
    private final ObjectTracker objectTracker;
    private final CommandFactory commandFactory;

    public SimpleRegistryProcessor(SimpleRegistry simpleRegistry, StorageTaskManager taskManager, ObjectTracker objectTracker, CommandFactory commandFactory) {
        this.simpleRegistry = simpleRegistry;
        this.commandFactory = commandFactory;
        this.objectTracker = objectTracker;
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
     * 注册时接收Storage节点汇报的对象列表，判断有哪些未被删除的对象
     * 将未删除对象添加到storage节点的任务队列中
     * @param context {@link ChannelHandlerContext}
     * @param command {@link TinyOssCommand}
     */
    private void processRegister(ChannelHandlerContext context, TinyOssCommand command){
        StorageNodeInfo storageNodeInfo = SerializeUtil.deserialize(command.getContent(), StorageNodeInfo.class);
        if(storageNodeInfo.getStoredObjects() != null){
            // 从storage汇报的对象中筛选出被删除的对象，然后包装成删除任务
            List<DeleteTask> deleteTasks = storageNodeInfo.getStoredObjects()
                    .stream()
                    .filter(objectTracker::isObjectDeleted).map(id -> new DeleteTask(0L, id))
                    .collect(Collectors.toList());
            // 添加到删除任务队列
            storageTaskManager.addDeleteTasks(storageNodeInfo.getUrl(), deleteTasks);
        }
        simpleRegistry.putStorageNode(storageNodeInfo);
        // 将channel和storageNode绑定，在连接断开后将节点从注册中心下线
        bindStorageNodeWithChannel(context.channel(), storageNodeInfo);
        log.info("Storage node online: {}", storageNodeInfo.getUrl());
        RemotingCommand response = commandFactory.createResponse(command.getId(), "", TinyOssProtocol.SUCCESS);
        sendResponse(context, response);
    }

    /**
     * 处理心跳
     * 通过心跳回复将副本复制任务和删除对象任务发送给storage节点
     * @param context {@link ChannelHandlerContext}
     * @param command {@link TinyOssCommand}
     */
    private void processHeartbeat(ChannelHandlerContext context, TinyOssCommand command){
        StorageNodeInfo storageNodeInfo = SerializeUtil.deserialize(command.getContent(), StorageNodeInfo.class);
        if(simpleRegistry.updateStorageNode(storageNodeInfo)){
            log.info("Storage node online: {}", storageNodeInfo.getUrl());
        }
        List<Long> storedObjects = storageNodeInfo.getStoredObjects();
        // 记录objects副本位置
        saveObjectReplicaLocations(storedObjects, storageNodeInfo.getUrl());
        // 绑定channel
        bindStorageNodeWithChannel(context.channel(), storageNodeInfo);
        // 从任务队列获取副本复制和删除任务
        List<ReplicaTask> replicaTasks = storageTaskManager.pollReplicaTasks(storageNodeInfo.getUrl(), 100);
        List<DeleteTask> deleteTasks = storageTaskManager.pollDeleteTask(storageNodeInfo.getUrl(), 100);
        // 通过心跳回复将任务发送给storage节点
        StorageHeartBeatResponse heartBeatResponse = new StorageHeartBeatResponse(replicaTasks, deleteTasks);
        byte[] content = SerializeUtil.serialize(heartBeatResponse, StorageHeartBeatResponse.class);
        RemotingCommand response = commandFactory.createResponse(command.getId(), content, TinyOssProtocol.SUCCESS);
        sendResponse(context, response);
    }

    /**
     * 将Channel与StorageNode绑定
     * 当Channel连接断开时通过绑定的url来将storageNode下线
     * @param channel {@link Channel}
     * @param storageNodeInfo {@link StorageNodeInfo}
     */
    private void bindStorageNodeWithChannel(Channel channel, StorageNodeInfo storageNodeInfo){
        // 判断是否有绑定channel
        Attribute<String> attr = channel.attr(SimpleRegistry.STORAGE_NODE_ATTR);
        if(attr.get() == null){
            attr.set(storageNodeInfo.getUrl());
        }
    }

    /**
     * 保存object副本的位置信息
     * @param storedObjects {@link List<Long>}
     * @param location 地址
     */
    private void saveObjectReplicaLocations(List<Long> storedObjects, String location){
        if(storedObjects != null && !storedObjects.isEmpty()){
            objectTracker.addObjectReplicasLocation(location, storedObjects);
        }
    }
}
