package com.jay.oss.storage.processor;

import com.jay.dove.transport.command.AbstractProcessor;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.constant.OssConstants;
import com.jay.oss.common.entity.request.UploadRequest;
import com.jay.oss.common.kafka.RecordProducer;
import com.jay.oss.common.remoting.TinyOssCommand;
import com.jay.oss.common.remoting.TinyOssProtocol;
import com.jay.oss.common.util.NodeInfoCollector;
import com.jay.oss.storage.fs.Block;
import com.jay.oss.storage.fs.BlockManager;
import com.jay.oss.storage.fs.ObjectIndex;
import com.jay.oss.storage.fs.ObjectIndexManager;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

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

    private final BlockManager blockManager;
    private final ObjectIndexManager objectIndexManager;
    private final CommandFactory commandFactory;
    private final RecordProducer storageNodeProducer;

    public FileUploadProcessor(ObjectIndexManager objectIndexManager, BlockManager blockManager, CommandFactory commandFactory, RecordProducer storageNodeProducer) {
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
                sendUploadCompleteRecord(objectId);
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
    private void sendUploadCompleteRecord(long objectId){
        storageNodeProducer.send(OssConstants.STORAGE_UPLOAD_COMPLETE, Long.toString(objectId), NodeInfoCollector.getAddress());
    }
}
