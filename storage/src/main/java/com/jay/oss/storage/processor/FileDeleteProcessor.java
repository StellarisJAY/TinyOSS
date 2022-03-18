package com.jay.oss.storage.processor;

import com.jay.dove.transport.command.AbstractProcessor;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.edit.EditLog;
import com.jay.oss.common.edit.EditLogManager;
import com.jay.oss.common.edit.EditOperation;
import com.jay.oss.common.entity.FileMetaWithChunkInfo;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.common.util.StringUtil;
import com.jay.oss.storage.fs.Chunk;
import com.jay.oss.storage.fs.ChunkManager;
import com.jay.oss.storage.meta.MetaManager;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

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
    private final EditLogManager editLogManager;

    public FileDeleteProcessor(ChunkManager chunkManager, MetaManager metaManager, EditLogManager editLogManager, CommandFactory commandFactory) {
        this.chunkManager = chunkManager;
        this.metaManager = metaManager;
        this.commandFactory = commandFactory;
        this.editLogManager = editLogManager;
        //Scheduler.scheduleAtFixedRate(new CompressionTask(), Chunk.CHUNK_COMPRESSION_PERIOD * 2, Chunk.CHUNK_COMPRESSION_PERIOD, TimeUnit.MILLISECONDS);
    }

    @Override
    public void process(ChannelHandlerContext channelHandlerContext, Object o) {
        if(o instanceof FastOssCommand){
            FastOssCommand command = (FastOssCommand) o;
            String objectKey = StringUtil.toString(command.getContent());
            // 删除meta
            FileMetaWithChunkInfo meta = metaManager.delete(objectKey);
            RemotingCommand response;
            if(meta == null){
                // 文件不存在
                response =  commandFactory.createResponse(command.getId(), "", FastOssProtocol.OBJECT_NOT_FOUND);
                sendResponse(channelHandlerContext, response);
                return;
            }else{
                meta.setRemoved(true);
                Chunk chunk = chunkManager.getChunkById(meta.getChunkId());
                if(chunk != null){
                    // 从chunk删除object，该方法会返回一个offset被删除过程改变的meta集合
                    List<FileMetaWithChunkInfo> offsetChangedMetas = chunk.deleteObject(meta);
                    // 记录offset改变的editLog
                    appendOffsetChangeLog(offsetChangedMetas);
                }
                // 记录删除editLog
                appendEditLog(objectKey);
                response = commandFactory.createResponse(command.getId(), "", FastOssProtocol.SUCCESS);
            }
            sendResponse(channelHandlerContext, response);
        }
    }

    private void appendEditLog(String key){
        byte[] keyBytes = key.getBytes(OssConfigs.DEFAULT_CHARSET);
        EditLog editLog = new EditLog(EditOperation.DELETE, keyBytes);
        editLogManager.append(editLog);
    }

    private void appendOffsetChangeLog(List<FileMetaWithChunkInfo> metas){
        if(metas != null && !metas.isEmpty()){
            for (FileMetaWithChunkInfo meta : metas) {
                byte[] serialized = SerializeUtil.serialize(meta, FileMetaWithChunkInfo.class);
                EditLog editLog = new EditLog(EditOperation.ADD, serialized);
                editLogManager.append(editLog);
            }
        }
    }
}
