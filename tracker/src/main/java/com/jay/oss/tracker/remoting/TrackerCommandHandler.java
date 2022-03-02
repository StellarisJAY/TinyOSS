package com.jay.oss.tracker.remoting;

import com.jay.dove.transport.command.AbstractCommandHandler;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.oss.common.edit.EditLogManager;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.tracker.meta.BucketManager;
import com.jay.oss.tracker.track.ObjectTracker;
import com.jay.oss.tracker.processor.BucketProcessor;
import com.jay.oss.tracker.registry.StorageRegistry;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/02/21 10:12
 */
public class TrackerCommandHandler extends AbstractCommandHandler {

    public TrackerCommandHandler(BucketManager bucketManager, ObjectTracker objectTracker, StorageRegistry storageRegistry,
                                 EditLogManager editLogManager, CommandFactory commandFactory) {
        super(commandFactory);
        BucketProcessor bucketProcessor = new BucketProcessor(bucketManager, storageRegistry, editLogManager,
                objectTracker, commandFactory);

        // 桶相关处理器
        this.registerProcessor(FastOssProtocol.PUT_BUCKET, bucketProcessor);
        this.registerProcessor(FastOssProtocol.LIST_BUCKET, bucketProcessor);
        this.registerProcessor(FastOssProtocol.CHECK_BUCKET_ACL, bucketProcessor);
        this.registerProcessor(FastOssProtocol.BUCKET_PUT_OBJECT, bucketProcessor);
        this.registerProcessor(FastOssProtocol.BUCKET_DELETE_OBJECT, bucketProcessor);
    }
}
