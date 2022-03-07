package com.jay.oss.storage.processor;

import com.jay.dove.DoveClient;
import com.jay.dove.transport.Url;
import com.jay.dove.transport.command.AbstractProcessor;
import com.jay.dove.transport.command.CommandCode;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.entity.AsyncBackupRequest;
import com.jay.oss.common.entity.FileMetaWithChunkInfo;
import com.jay.oss.common.entity.UploadRequest;
import com.jay.oss.common.fs.Chunk;
import com.jay.oss.common.fs.ChunkManager;
import com.jay.oss.common.fs.FilePartWrapper;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.storage.meta.MetaManager;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * <p>
 *  异步备份处理器
 * </p>
 *
 * @author Jay
 * @date 2022/03/07 11:52
 */
@Slf4j
public class AsyncBackupProcessor extends AbstractProcessor {
    private final DoveClient client;
    private final MetaManager metaManager;
    private final ChunkManager chunkManager;

    public AsyncBackupProcessor(DoveClient client, MetaManager metaManager, ChunkManager chunkManager) {
        this.client = client;
        this.metaManager = metaManager;
        this.chunkManager = chunkManager;
    }

    @Override
    public void process(ChannelHandlerContext channelHandlerContext, Object o) {
        if(o instanceof FastOssCommand){
            FastOssCommand command = (FastOssCommand) o;
            CommandCode code = command.getCommandCode();
            if(FastOssProtocol.ASYNC_BACKUP.equals(code)){
                processAsyncBackup(channelHandlerContext, command);
            }
        }
    }

    private void processAsyncBackup(ChannelHandlerContext context, FastOssCommand command){
        byte[] content = command.getContent();
        AsyncBackupRequest request = SerializeUtil.deserialize(content, AsyncBackupRequest.class);
        String objectKey = request.getObjectKey();
        FileMetaWithChunkInfo meta = metaManager.getMeta(objectKey);
        List<String> targets = request.getTargets();
        if(meta != null){
            Chunk chunk = chunkManager.getChunkById(meta.getChunkId());
            if(chunk != null){
                UploadRequest uploadRequest = UploadRequest.builder()
                        .key(objectKey).parts(1)
                        .size(meta.getSize()).filename(meta.getFilename())
                        .build();
                for (String target : targets) {
                    Url url = Url.parseString(target);
                    try{
                        FastOssCommand headerResponse = sendHeader(url, uploadRequest);
                        if(headerResponse.getCommandCode().equals(FastOssProtocol.SUCCESS)){
                            FastOssCommand dataResponse = sendObject(url, objectKey, chunk, meta);
                        }
                    }catch (Exception e){
                        log.warn("Send Backup Failed, object: {}, target Node: {}", objectKey, target);
                    }
                }
            }else{
                log.warn("Can't send backup for object: {}, object data not found", objectKey);
            }
        }else{
            log.warn("Can't send backup for object: {}, object not found", objectKey);
        }
    }

    private FastOssCommand sendHeader(Url url, UploadRequest request) throws Exception {
        byte[] content = SerializeUtil.serialize(request, UploadRequest.class);
        FastOssCommand command = (FastOssCommand) client.getCommandFactory()
                .createRequest(content, FastOssProtocol.UPLOAD_FILE_HEADER);
        return (FastOssCommand) client.sendSync(url, command, null);
    }

    private FastOssCommand sendObject(Url url, String objectKey, Chunk chunk, FileMetaWithChunkInfo meta) throws Exception {
        byte[] keyBytes = objectKey.getBytes(OssConfigs.DEFAULT_CHARSET);
        ByteBuf fullContent = chunk.readFileBytes(objectKey, meta.getOffset(), meta.getSize());
        FilePartWrapper wrapper = FilePartWrapper.builder()
                .key(keyBytes).keyLength(keyBytes.length)
                .index(0).length((int) meta.getSize())
                .partNum(1).fullContent(fullContent)
                .build();
        FastOssCommand command = (FastOssCommand) client.getCommandFactory()
                .createRequest(wrapper, FastOssProtocol.UPLOAD_FILE_PARTS);
        return (FastOssCommand) client.sendSync(url, command, null);
    }
}
