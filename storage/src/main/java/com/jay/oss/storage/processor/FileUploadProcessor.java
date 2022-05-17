package com.jay.oss.storage.processor;

import com.jay.dove.DoveClient;
import com.jay.dove.transport.command.AbstractProcessor;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.constant.OssConstants;
import com.jay.oss.common.entity.request.StartCopyReplicaRequest;
import com.jay.oss.common.entity.request.UploadRequest;
import com.jay.oss.common.kafka.RecordProducer;
import com.jay.oss.common.remoting.TinyOssCommand;
import com.jay.oss.common.remoting.TinyOssProtocol;
import com.jay.oss.common.util.NodeInfoCollector;
import com.jay.oss.common.util.StringUtil;
import com.jay.oss.storage.fs.Block;
import com.jay.oss.storage.fs.BlockManager;
import com.jay.oss.storage.fs.ObjectIndex;
import com.jay.oss.storage.fs.ObjectIndexManager;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>
 *  文件上传处理器
 * </p>
 *
 * @author Jay
 * @date 2022/01/18 14:45
 */
@Slf4j
public class FileUploadProcessor extends AbstractProcessor {

    private final DoveClient trackerClient;
    private final BlockManager blockManager;
    private final ObjectIndexManager objectIndexManager;
    private final CommandFactory commandFactory;
    private final RecordProducer storageNodeProducer;

    public FileUploadProcessor(DoveClient trackerClient, ObjectIndexManager objectIndexManager, BlockManager blockManager, CommandFactory commandFactory, RecordProducer storageNodeProducer) {
        this.trackerClient = trackerClient;
        this.objectIndexManager = objectIndexManager;
        this.commandFactory = commandFactory;
        this.storageNodeProducer = storageNodeProducer;
        this.blockManager = blockManager;
    }

    @Override
    public void process(ChannelHandlerContext channelHandlerContext, Object o) {
        if(o instanceof TinyOssCommand){
            TinyOssCommand command = (TinyOssCommand) o;
            CommandCode commandCode = command.getCommandCode();
            if(TinyOssProtocol.UPLOAD_REQUEST.equals(commandCode)){
                processUploadRequest(channelHandlerContext, command);
            }
        }
    }


    /**
     * 处理上传请求
     * @param context context
     * @param command {@link TinyOssCommand}
     */
    private void processUploadRequest(ChannelHandlerContext context, TinyOssCommand command){
        ByteBuf data = command.getData();
        try{
            if(data.readableBytes() <= UploadRequest.HEADER_LENGTH){
                return;
            }
            long objectId = data.readLong();
            long size = data.readLong();
            AtomicBoolean duplicateObject = new AtomicBoolean(true);
        /*
            computeIfAbsent 保证同一个key的meta只保存一次
         */
            objectIndexManager.computeIfAbsent(objectId, (id)->{
                // 获取chunk文件
                Block block = blockManager.getBlockBySize((int)size);
                ObjectIndex index = block.write(id, data, (int) size);
                duplicateObject.set(false);
                blockManager.offerBlock(block);
                return index;
            });
            // 没能够成功进行computeIfAbsent的重复的key
            if(duplicateObject.get()){
                // 发送重复回复报文
                RemotingCommand response = commandFactory.createResponse(command.getId(), "", TinyOssProtocol.ERROR);
                sendResponse(context, response);
            } else{
                data.skipBytes((int)size);
                byte[] replicaLocationBytes = new byte[data.readableBytes()];
                data.readBytes(replicaLocationBytes);
                String[] locations = StringUtil.toString(replicaLocationBytes).split(";");
                sendUploadCompleteRecord(objectId, Arrays.asList(locations));
                RemotingCommand response = commandFactory.createResponse(command.getId(), "", TinyOssProtocol.SUCCESS);
                sendResponse(context, response);
            }
        }finally {
            data.release();
        }
    }

    /**
     * 向Tracker发送接收完成的消息，通知Tracker下发副本复制指令
     * @param objectId 对象ID
     */
    private void sendUploadCompleteRecord(long objectId, List<String> locations){
        if(OssConfigs.enableTrackerMessaging()){
            try{
                StartCopyReplicaRequest request = new StartCopyReplicaRequest(objectId, NodeInfoCollector.getAddress(), locations);
                RemotingCommand command = commandFactory.createRequest(request, TinyOssProtocol.UPLOAD_COMPLETE, StartCopyReplicaRequest.class);
                RemotingCommand response = trackerClient.sendSync(OssConfigs.trackerServerUrl(), command, null);
                if(!response.getCommandCode().equals(TinyOssProtocol.SUCCESS)){
                    log.warn("Start copy replica for {} failed", objectId);
                }
            }catch (Exception e){
                log.warn("Error when sending start copy replica for {} ", objectId, e);
            }
        }else{
            storageNodeProducer.send(OssConstants.STORAGE_UPLOAD_COMPLETE, Long.toString(objectId), NodeInfoCollector.getAddress());
        }
    }
}
