package com.jay.oss.storage.processor;

import com.jay.dove.transport.command.AbstractProcessor;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.edit.EditLogManager;
import com.jay.oss.common.entity.request.CompleteMultipartUploadRequest;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.storage.fs.Block;
import com.jay.oss.storage.fs.BlockManager;
import com.jay.oss.storage.meta.MetaManager;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * <p>
 *  分片上传请求处理器
 *  目前暂不支持分片上传
 * </p>
 *
 * @author Jay
 * @date 2022/03/09 11:17
 */
@Slf4j
public class MultipartUploadProcessor extends AbstractProcessor {

    private final BlockManager blockManager;
    private final MetaManager metaManager;
    private final EditLogManager editLogManager;
    private final CommandFactory commandFactory;

    public MultipartUploadProcessor(BlockManager blockManager, MetaManager metaManager, EditLogManager editLogManager, CommandFactory commandFactory) {
        this.blockManager = blockManager;
        this.metaManager = metaManager;
        this.editLogManager = editLogManager;
        this.commandFactory = commandFactory;
    }

    @Override
    public void process(ChannelHandlerContext context, Object o) {
        if(o instanceof FastOssCommand){
            FastOssCommand command = (FastOssCommand) o;
            CommandCode code = command.getCommandCode();
        }
    }

    /**
     * 处理上传分片
     * @param context {@link ChannelHandlerContext}
     * @param command {@link FastOssCommand}
     */
    private void processUploadPart(ChannelHandlerContext context, FastOssCommand command){

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
    private void abortMultipartUpload(ChannelHandlerContext context, FastOssCommand command){

    }


    /**
     * 合并临时block
     * @param tempBlocks 临时block集合
     * @param size 总共大小
     * @param request {@link CompleteMultipartUploadRequest}
     * @param requestId 请求ID
     * @return {@link RemotingCommand}
     */
    private RemotingCommand mergeTempBlocks(List<Block> tempBlocks, long size, CompleteMultipartUploadRequest request, int requestId){
        return null;
    }
}
