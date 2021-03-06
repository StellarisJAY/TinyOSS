package com.jay.oss.storage.processor;

import com.jay.dove.transport.command.AbstractProcessor;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.oss.common.entity.request.GetObjectRequest;
import com.jay.oss.common.remoting.TinyOssCommand;
import com.jay.oss.common.remoting.TinyOssProtocol;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.storage.fs.Block;
import com.jay.oss.storage.fs.BlockManager;
import com.jay.oss.storage.fs.ObjectIndex;
import com.jay.oss.storage.fs.ObjectIndexManager;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 *  下载请求处理器
 * </p>
 *
 * @author Jay
 * @date 2022/02/11 11:04
 */
@Slf4j
public class FileDownloadProcessor extends AbstractProcessor {

    private final ObjectIndexManager objectIndexManager;
    private final BlockManager blockManager;
    private final CommandFactory commandFactory;

    public FileDownloadProcessor(ObjectIndexManager objectIndexManager, BlockManager blockManager, CommandFactory commandFactory) {
        this.objectIndexManager = objectIndexManager;
        this.blockManager = blockManager;
        this.commandFactory = commandFactory;
    }

    @Override
    public void process(ChannelHandlerContext channelHandlerContext, Object o) {
        if(o instanceof TinyOssCommand){
            TinyOssCommand command = (TinyOssCommand) o;
            processDownload(channelHandlerContext, command);
        }
    }

    /**
     * 处理下载请求
     * @param context {@link ChannelHandlerContext}
     * @param command {@link TinyOssCommand}
     */
    private void processDownload(ChannelHandlerContext context, TinyOssCommand command){
        GetObjectRequest request = SerializeUtil.deserialize(command.getContent(), GetObjectRequest.class);
        ObjectIndex objectIndex = objectIndexManager.getObjectIndex(request.getObjectId());
        if(objectIndex == null){
            sendResponse(context, commandFactory.createResponse(command.getId(), "", TinyOssProtocol.OBJECT_NOT_FOUND));
            return;
        }
        Block block = blockManager.getBlockById(objectIndex.getBlockId());
        if(block != null){
            int readStart = request.getEnd() == -1 ? 0 : request.getStart();
            int readLength = request.getEnd() == -1 ? objectIndex.getSize() : request.getEnd() - request.getStart();
            ByteBuf buffer = block.read(objectIndex.getOffset(), readStart, readLength);
            sendResponse(context, commandFactory.createResponse(command.getId(), buffer, TinyOssProtocol.DOWNLOAD_RESPONSE));
        }else{
            sendResponse(context, commandFactory.createResponse(command.getId(), "", TinyOssProtocol.ERROR));
        }
    }
}
