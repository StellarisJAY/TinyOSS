package com.jay.oss.storage.command;

import com.jay.dove.transport.command.CommandFactory;
import com.jay.oss.common.kafka.RecordProducer;
import com.jay.oss.common.remoting.TinyOssCommandHandler;
import com.jay.oss.common.remoting.TinyOssProtocol;
import com.jay.oss.storage.fs.BlockManager;
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
public class StorageNodeCommandHandler extends TinyOssCommandHandler {

    public StorageNodeCommandHandler(CommandFactory commandFactory, ExecutorService executor, MetaManager metaManager,
                                     RecordProducer storageNodeProducer, BlockManager blockManager) {
        super(commandFactory, executor);
        // 文件上传处理器
        FileUploadProcessor fileUploadProcessor = new FileUploadProcessor(metaManager, blockManager, commandFactory, storageNodeProducer);
        FileDownloadProcessor fileDownloadProcessor = new FileDownloadProcessor(metaManager, blockManager, commandFactory);
        /*
            Put Object处理器
         */
        this.registerProcessor(TinyOssProtocol.UPLOAD_FILE_HEADER, fileUploadProcessor);
        this.registerProcessor(TinyOssProtocol.UPLOAD_FILE_PARTS, fileUploadProcessor);
        this.registerProcessor(TinyOssProtocol.UPLOAD_REQUEST, fileUploadProcessor);
        /*
            Get Object 处理器
         */
        this.registerProcessor(TinyOssProtocol.DOWNLOAD_FULL, fileDownloadProcessor);
        this.registerProcessor(TinyOssProtocol.DOWNLOAD_RANGED, fileDownloadProcessor);
    }
}
