package com.jay.oss.storage.processor;

import com.jay.dove.transport.command.AbstractProcessor;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.oss.common.entity.request.GetObjectRequest;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.storage.fs.Block;
import com.jay.oss.storage.fs.BlockManager;
import com.jay.oss.storage.fs.ObjectIndex;
import com.jay.oss.storage.meta.MetaManager;
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

    private final MetaManager metaManager;
    private final BlockManager blockManager;
    private final CommandFactory commandFactory;

    public FileDownloadProcessor(MetaManager metaManager, BlockManager blockManager, CommandFactory commandFactory) {
        this.metaManager = metaManager;
        this.blockManager = blockManager;
        this.commandFactory = commandFactory;
    }

    @Override
    public void process(ChannelHandlerContext channelHandlerContext, Object o) {
        if(o instanceof FastOssCommand){
            FastOssCommand command = (FastOssCommand) o;
            processDownload(channelHandlerContext, command);
        }
    }

    private void processDownload(ChannelHandlerContext context, FastOssCommand command){
        GetObjectRequest request = SerializeUtil.deserialize(command.getContent(), GetObjectRequest.class);
        ObjectIndex objectIndex = metaManager.getObjectIndex(request.getObjectId());
        if(objectIndex == null){
            log.warn("Object not found, id: {}", request.getObjectId());
            sendResponse(context, commandFactory.createResponse(command.getId(), "", FastOssProtocol.OBJECT_NOT_FOUND));
            return;
        }
        Block block = blockManager.getBlockById(objectIndex.getBlockId());
        if(block != null){
            int readStart = request.getEnd() == -1 ? 0 : request.getStart();
            int readLength = request.getEnd() == -1 ? objectIndex.getSize() : request.getEnd() - request.getStart();
            ByteBuf buffer = block.read(objectIndex.getOffset(), readStart, readLength);
            sendResponse(context, commandFactory.createResponse(command.getId(), buffer, FastOssProtocol.DOWNLOAD_RESPONSE));
        }else{
            sendResponse(context, commandFactory.createResponse(command.getId(), "", FastOssProtocol.ERROR));
        }
    }
}
