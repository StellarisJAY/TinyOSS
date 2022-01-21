package com.jay.oss.storage.command;

import com.jay.dove.transport.command.CommandFactory;
import com.jay.oss.common.remoting.FastOssCommandHandler;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.storage.processor.FileUploadProcessor;

import java.util.concurrent.ExecutorService;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/01/18 14:42
 */
public class StorageNodeCommandHandler extends FastOssCommandHandler {

    public StorageNodeCommandHandler(CommandFactory commandFactory, ExecutorService executor) {
        super(commandFactory, executor);
        FileUploadProcessor fileUploadProcessor = new FileUploadProcessor();

        this.registerProcessor(FastOssProtocol.UPLOAD_FILE_HEADER, fileUploadProcessor);
        this.registerProcessor(FastOssProtocol.UPLOAD_FILE_PARTS, fileUploadProcessor);


    }
}
