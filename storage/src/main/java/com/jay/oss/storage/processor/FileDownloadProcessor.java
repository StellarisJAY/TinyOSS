package com.jay.oss.storage.processor;

import com.jay.dove.serialize.Serializer;
import com.jay.dove.serialize.SerializerManager;
import com.jay.dove.transport.command.AbstractProcessor;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.oss.common.entity.DownloadRequest;
import com.jay.oss.common.entity.FileMetaWithChunkInfo;
import com.jay.oss.common.fs.Chunk;
import com.jay.oss.common.fs.ChunkManager;
import com.jay.oss.common.fs.FileChunkIndex;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.storage.meta.MetaManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
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
    private final ChunkManager chunkManager;
    private final CommandFactory commandFactory;

    public FileDownloadProcessor(MetaManager metaManager, ChunkManager chunkManager, CommandFactory commandFactory) {
        this.metaManager = metaManager;
        this.chunkManager = chunkManager;
        this.commandFactory = commandFactory;
    }

    @Override
    public void process(ChannelHandlerContext channelHandlerContext, Object o) {
        if(o instanceof FastOssCommand){
            FastOssCommand command = (FastOssCommand) o;
            byte[] content = command.getContent();
            // 反序列化request
            Serializer serializer = SerializerManager.getSerializer(command.getSerializer());
            DownloadRequest request = serializer.deserialize(content, DownloadRequest.class);

            processDownloadFull(channelHandlerContext, command.getId(), request);
        }
    }

    private void processDownloadFull(ChannelHandlerContext context, int requestId, DownloadRequest request){
        FastOssCommand response = null;
        try{
            String key = request.getKey();
            int start = request.getStart();
            // 获取文件meta
            FileMetaWithChunkInfo meta = metaManager.getMeta(key);
            if(meta == null){
                response = (FastOssCommand) commandFactory.createResponse(requestId, "object not found", FastOssProtocol.OBJECT_NOT_FOUND);
                return;
            }
            // 获取chunk索引
            FileChunkIndex chunkIndex = meta.getChunkIndex();
            // 获取chunk
            Chunk chunk = chunkManager.getChunkById(chunkIndex.getChunkId());
            // 根据下载类型计算读取长度
            int readLength = (int)(request.isFull() ? chunkIndex.getSize() : request.getLength());
            // 从chunk读取数据
            ByteBuf data = Unpooled.directBuffer(readLength);
            chunk.read(key, chunkIndex.getOffset() + start, readLength, data);
            response = (FastOssCommand) commandFactory.createResponse(requestId, data, FastOssProtocol.DOWNLOAD_RESPONSE);
        }catch (Exception e){
            log.error("process download request error: ", e);
            response = (FastOssCommand) commandFactory.createExceptionResponse(requestId, e);
        }finally {
            sendResponse(context, response);
        }
    }
}
