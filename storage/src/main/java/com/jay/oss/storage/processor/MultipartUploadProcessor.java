package com.jay.oss.storage.processor;

import com.jay.dove.transport.command.AbstractProcessor;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.edit.EditLog;
import com.jay.oss.common.edit.EditLogManager;
import com.jay.oss.common.edit.EditOperation;
import com.jay.oss.common.entity.CompleteMultipartUploadRequest;
import com.jay.oss.common.entity.FileMetaWithChunkInfo;
import com.jay.oss.common.fs.Chunk;
import com.jay.oss.common.fs.ChunkManager;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.common.util.StringUtil;
import com.jay.oss.storage.meta.MetaManager;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

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
                abortMultipartUpload(context, command);
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
        CompleteMultipartUploadRequest request = SerializeUtil.deserialize(command.getContent(), CompleteMultipartUploadRequest.class);
        String uploadId = request.getUploadId();
        int parts = request.getParts();
        List<Chunk> chunks = new ArrayList<>(parts);
        long size = 0;
        for (int i = 1; i <= parts; i++) {
            // 找到分片的临时chunk
            Chunk tempChunk = chunkManager.getTempChunk(uploadId, i);
            if(tempChunk == null){
                // 临时chunk不存在，表示分片丢失，自动中止MultipartUpload
                doAbortMultipartUpload(uploadId, parts);
                return;
            }
            else{
                chunks.add(tempChunk);
                size += tempChunk.size();
            }
        }
        // 合并临时chunk
        RemotingCommand response = mergeTempChunks(chunks, size, request, command.getId());
        sendResponse(context, response);
    }

    /**
     * 处理取消分片上传
     * @param context {@link ChannelHandlerContext}
     * @param command {@link FastOssCommand}
     */
    private void abortMultipartUpload(ChannelHandlerContext context, FastOssCommand command){

    }

    private void doAbortMultipartUpload(String uploadId, int parts){

    }

    private RemotingCommand mergeTempChunks(List<Chunk> tempChunks, long size, CompleteMultipartUploadRequest request, int requestId){
        AtomicBoolean duplicateObject = new AtomicBoolean(true);
        AtomicBoolean mergeError = new AtomicBoolean(false);
        RemotingCommand response;
        metaManager.computeIfAbsent(request.getObjectKey(), k->{
            // 获取新chunk
            Chunk mergedChunk = chunkManager.getChunkBySize(size);
            int offset = mergedChunk.getAndAddSize((int)size);
            int transferred = 0;
            FileMetaWithChunkInfo meta = FileMetaWithChunkInfo.builder()
                    .filename(request.getFilename())
                    .size(size).createTime(System.currentTimeMillis())
                    .key(request.getObjectKey())
                    .chunkId(mergedChunk.getId()).offset(offset)
                    .removed(false)
                    .build();
            try{
                // 合并tempChunk到目标chunk
                for (Chunk tempChunk : tempChunks) {
                    mergedChunk.transferFrom(tempChunk, offset + transferred, tempChunk.size());
                    transferred += tempChunk.size();
                }
                // 放回chunk
                chunkManager.offerChunk(mergedChunk);
                appendPutObjectLog(meta);
                // 销毁临时chunk
                destroyTempChunks(tempChunks);
            }catch (IOException e){
                log.error("Merge Temp Chunk Failed, uploadId:{} ", request.getUploadId(), e);
                mergeError.set(true);
            }
            duplicateObject.set(false);
            return meta;
        });

        if(!mergeError.get() && !duplicateObject.get()){
            response = commandFactory.createResponse(requestId, "", FastOssProtocol.SUCCESS);
        }else if(duplicateObject.get()){
            response = commandFactory.createResponse(requestId, "", FastOssProtocol.DUPLICATE_OBJECT_KEY);
        }else{
            response = commandFactory.createResponse(requestId, "", FastOssProtocol.ERROR);
        }
        return response;
    }

    private void destroyTempChunks(List<Chunk> tempChunks){
        for (Chunk tempChunk : tempChunks) {
            tempChunk.destroy();
        }
    }

    private void appendPutObjectLog(FileMetaWithChunkInfo meta){
        byte[] serialized = SerializeUtil.serialize(meta, FileMetaWithChunkInfo.class);
        // 生成编辑日志
        EditLog editLog = new EditLog(EditOperation.ADD, serialized);
        editLogManager.append(editLog);
    }
}
