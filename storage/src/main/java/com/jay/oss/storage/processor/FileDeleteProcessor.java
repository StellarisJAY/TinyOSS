package com.jay.oss.storage.processor;

import com.jay.dove.serialize.Serializer;
import com.jay.dove.serialize.SerializerManager;
import com.jay.dove.transport.command.AbstractProcessor;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.oss.common.entity.DeleteRequest;
import com.jay.oss.common.entity.FileMetaWithChunkInfo;
import com.jay.oss.common.fs.ChunkManager;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.storage.meta.MetaManager;
import io.netty.channel.ChannelHandlerContext;

/**
 * <p>
 *  删除文件处理器
 * </p>
 *
 * @author Jay
 * @date 2022/02/12 12:45
 */
public class FileDeleteProcessor extends AbstractProcessor {

    private final ChunkManager chunkManager;
    private final MetaManager metaManager;
    private final CommandFactory commandFactory;

    public FileDeleteProcessor(ChunkManager chunkManager, MetaManager metaManager, CommandFactory commandFactory) {
        this.chunkManager = chunkManager;
        this.metaManager = metaManager;
        this.commandFactory = commandFactory;
        //Scheduler.scheduleAtFixedRate(new CompressionTask(), Chunk.CHUNK_COMPRESSION_PERIOD * 2, Chunk.CHUNK_COMPRESSION_PERIOD, TimeUnit.MILLISECONDS);
    }

    @Override
    public void process(ChannelHandlerContext channelHandlerContext, Object o) {
        if(o instanceof FastOssCommand){
            FastOssCommand command = (FastOssCommand) o;
            byte[] content = command.getContent();
            // 反序列化请求
            Serializer serializer = SerializerManager.getSerializer(command.getSerializer());
            DeleteRequest request = serializer.deserialize(content, DeleteRequest.class);
            // 删除meta
            FileMetaWithChunkInfo meta = metaManager.delete(request.getKey());
            FastOssCommand response;
            if(meta == null){
                // 文件不存在
                response = (FastOssCommand) commandFactory.createResponse(command.getId(), "object not found", FastOssProtocol.OBJECT_NOT_FOUND);
                sendResponse(channelHandlerContext, response);
                return;
            }else{
                meta.setRemoved(true);
                response = (FastOssCommand)commandFactory.createResponse(command.getId(), "set delete tag error", FastOssProtocol.ERROR);
            }
            sendResponse(channelHandlerContext, response);
        }
    }
}
