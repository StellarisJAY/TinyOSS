package com.jay.oss.storage.command;

import com.jay.dove.DoveClient;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.oss.common.edit.EditLogManager;
import com.jay.oss.common.kafka.RecordProducer;
import com.jay.oss.storage.fs.BlockManager;
import com.jay.oss.storage.fs.ChunkManager;
import com.jay.oss.common.remoting.FastOssCommandHandler;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.storage.meta.MetaManager;
import com.jay.oss.storage.processor.*;

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

    public StorageNodeCommandHandler(CommandFactory commandFactory, ExecutorService executor,
                                     ChunkManager chunkManager, MetaManager metaManager, EditLogManager editLogManager,
                                     DoveClient client, RecordProducer storageNodeProducer, BlockManager blockManager) {
        super(commandFactory, executor);
        // 文件上传处理器
        FileUploadProcessor fileUploadProcessor = new FileUploadProcessor(chunkManager, metaManager, editLogManager, blockManager, commandFactory, storageNodeProducer);
        FileDownloadProcessor fileDownloadProcessor = new FileDownloadProcessor(metaManager, chunkManager, blockManager, commandFactory);
        FileDeleteProcessor fileDeleteProcessor = new FileDeleteProcessor(chunkManager, metaManager, editLogManager, commandFactory);
        AsyncBackupProcessor asyncBackupProcessor = new AsyncBackupProcessor(client, metaManager, chunkManager);
        MultipartUploadProcessor multipartUploadProcessor = new MultipartUploadProcessor(chunkManager, metaManager, editLogManager, commandFactory);
        /*
            Put Object处理器
         */
        this.registerProcessor(FastOssProtocol.UPLOAD_FILE_HEADER, fileUploadProcessor);
        this.registerProcessor(FastOssProtocol.UPLOAD_FILE_PARTS, fileUploadProcessor);
        this.registerProcessor(FastOssProtocol.UPLOAD_REQUEST, fileUploadProcessor);
        /*
            Get Object 处理器
         */
        this.registerProcessor(FastOssProtocol.DOWNLOAD_FULL, fileDownloadProcessor);
        this.registerProcessor(FastOssProtocol.DOWNLOAD_RANGED, fileDownloadProcessor);
        this.registerProcessor(FastOssProtocol.DELETE_OBJECT, fileDeleteProcessor);
        /*
            异步备份处理器
         */
        this.registerProcessor(FastOssProtocol.ASYNC_BACKUP, asyncBackupProcessor);
        this.registerProcessor(FastOssProtocol.ASYNC_BACKUP_PART, asyncBackupProcessor);
        /*
            分片上传处理器
         */
        this.registerProcessor(FastOssProtocol.MULTIPART_UPLOAD_PART, multipartUploadProcessor);
        this.registerProcessor(FastOssProtocol.COMPLETE_MULTIPART_UPLOAD, multipartUploadProcessor);
        this.registerProcessor(FastOssProtocol.CANCEL_MULTIPART_UPLOAD, multipartUploadProcessor);
    }
}
