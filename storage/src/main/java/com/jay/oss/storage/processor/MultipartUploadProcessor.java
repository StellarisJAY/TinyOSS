package com.jay.oss.storage.processor;

import com.jay.dove.transport.command.AbstractProcessor;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.edit.EditLogManager;
import com.jay.oss.common.fs.Chunk;
import com.jay.oss.common.fs.ChunkManager;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.util.StringUtil;
import com.jay.oss.storage.meta.MetaManager;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * <p>
 *  分片上传请求处理器
 * </p>
 *
 * @author Jay
 * @date 2022/03/09 11:17
 */
@Slf4j
public class MultipartUploadProcessor extends AbstractProcessor {

    private final ChunkManager chunkManager;
    private final MetaManager metaManager;
    private final EditLogManager editLogManager;
    private final CommandFactory commandFactory;

    public MultipartUploadProcessor(ChunkManager chunkManager, MetaManager metaManager, EditLogManager editLogManager, CommandFactory commandFactory) {
        this.chunkManager = chunkManager;
        this.metaManager = metaManager;
        this.editLogManager = editLogManager;
        this.commandFactory = commandFactory;
    }

    @Override
    public void process(ChannelHandlerContext context, Object o) {
        if(o instanceof FastOssCommand){
            FastOssCommand command = (FastOssCommand) o;
            CommandCode code = command.getCommandCode();
            if(FastOssProtocol.MULTIPART_UPLOAD_PART.equals(code)){
                processUploadPart(context, command);
            }
            else if(FastOssProtocol.COMPLETE_MULTIPART_UPLOAD.equals(code)){
                completeMultipartUpload(context, command);
            }
            else if(FastOssProtocol.CANCEL_MULTIPART_UPLOAD.equals(code)){
                cancelMultipartUpload(context, command);
            }
        }
    }

    /**
     * 处理上传分片
     * @param context {@link ChannelHandlerContext}
     * @param command {@link FastOssCommand}
     */
    private void processUploadPart(ChannelHandlerContext context, FastOssCommand command){
        ByteBuf data = command.getData();
        int keyLen = data.readInt();
        String objectKey = StringUtil.readString(data, keyLen);
        int partNum = data.readInt();
        RemotingCommand response;
        try{
            Chunk tempChunk = chunkManager.getTempChunkAndCreateIfAbsent(objectKey, partNum);
            tempChunk.write(data);
            response = commandFactory.createResponse(command.getId(), "", FastOssProtocol.SUCCESS);
        }catch (IOException e){
            log.error("Failed to Write Object Part, object key: {}, partNum: {}", objectKey, partNum, e);
            response = commandFactory.createResponse(command.getId(), "", FastOssProtocol.ERROR);
        }
        sendResponse(context, response);
    }

    /**
     * 处理分片上传完成
     * @param context {@link ChannelHandlerContext}
     * @param command {@link FastOssCommand}
     */
    private void completeMultipartUpload(ChannelHandlerContext context, FastOssCommand command){

    }

    /**
     * 处理取消分片上传
     * @param context {@link ChannelHandlerContext}
     * @param command {@link FastOssCommand}
     */
    private void cancelMultipartUpload(ChannelHandlerContext context, FastOssCommand command){

    }
}
