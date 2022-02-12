package com.jay.oss.storage.command;

import com.jay.dove.transport.command.CommandFactory;
import com.jay.oss.common.fs.ChunkManager;
import com.jay.oss.common.remoting.FastOssCommandHandler;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.storage.meta.MetaManager;
import com.jay.oss.storage.processor.FileDownloadProcessor;
import com.jay.oss.storage.processor.FileUploadProcessor;

import java.util.concurrent.ExecutorService;

/**
 * <p>
 *  存储节点CommandHandler。
 *  除了默认的处理器外，添加了存储节点相关的处理器
 * </p>
 *
 * @author Jay
 * @date 2022/01/18 14:42
 */
public class StorageNodeCommandHandler extends FastOssCommandHandler {

    public StorageNodeCommandHandler(CommandFactory commandFactory, ExecutorService executor, ChunkManager chunkManager, MetaManager metaManager) {
        super(commandFactory, executor);
        // 文件上传处理器
        FileUploadProcessor fileUploadProcessor = new FileUploadProcessor(chunkManager, metaManager, commandFactory);
        FileDownloadProcessor fileDownloadProcessor = new FileDownloadProcessor(metaManager, chunkManager, commandFactory);
        this.registerProcessor(FastOssProtocol.UPLOAD_FILE_HEADER, fileUploadProcessor);
        this.registerProcessor(FastOssProtocol.UPLOAD_FILE_PARTS, fileUploadProcessor);
        this.registerProcessor(FastOssProtocol.DOWNLOAD_FULL, fileDownloadProcessor);
        this.registerProcessor(FastOssProtocol.DOWNLOAD_RANGED, fileDownloadProcessor);
    }
}
