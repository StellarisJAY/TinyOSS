package com.jay.oss.storage.processor;

import com.jay.dove.serialize.Serializer;
import com.jay.dove.serialize.SerializerManager;
import com.jay.dove.transport.command.AbstractProcessor;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.entity.request.DownloadRequest;
import com.jay.oss.common.entity.FileMetaWithChunkInfo;
import com.jay.oss.common.entity.request.GetObjectRequest;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.storage.fs.*;
import com.jay.oss.storage.meta.MetaManager;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;
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
    private final ChunkManager chunkManager;
    private final BlockManager blockManager;
    private final CommandFactory commandFactory;

    public FileDownloadProcessor(MetaManager metaManager, ChunkManager chunkManager, BlockManager blockManager, CommandFactory commandFactory) {
        this.metaManager = metaManager;
        this.chunkManager = chunkManager;
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
            ByteBuf buffer = block.mmapReadBytes(objectIndex.getOffset(), readStart, readLength);
            sendResponse(context, commandFactory.createResponse(command.getId(), buffer, FastOssProtocol.DOWNLOAD_RESPONSE));
        }else{
            sendResponse(context, commandFactory.createResponse(command.getId(), "", FastOssProtocol.ERROR));
        }
    }

    private void processDownloadFull(ChannelHandlerContext context, int requestId, DownloadRequest request){
        RemotingCommand response;
        try{
            String key = request.getKey();
            int start = request.getStart();
            // 获取文件meta
            FileMetaWithChunkInfo meta = metaManager.getMeta(key);
            if(meta == null){
                response = commandFactory.createResponse(requestId, "object not found", FastOssProtocol.OBJECT_NOT_FOUND);
            }else{
                // 获取chunk
                Chunk chunk = chunkManager.getChunkById(meta.getChunkId());
                // 根据下载类型计算读取长度
                int readLength = (int)(request.isFull() ? meta.getSize() : request.getLength());
                ByteBuf buffer = chunk.readFileBytes(key, start + meta.getOffset(), readLength);
                response = commandFactory.createResponse(requestId, buffer, FastOssProtocol.DOWNLOAD_RESPONSE);
            }
            sendResponse(context, response);
        }catch (Exception e){
            log.error("process download request error: ", e);
            response = commandFactory.createExceptionResponse(requestId, e);
            sendResponse(context, response);
        }
    }
}
