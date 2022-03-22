package com.jay.oss.tracker.remoting;

import com.jay.dove.transport.command.AbstractCommandHandler;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.oss.common.edit.EditLogManager;
import com.jay.oss.common.kafka.RecordProducer;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.tracker.db.SqlUtil;
import com.jay.oss.tracker.meta.BucketManager;
import com.jay.oss.tracker.processor.BucketProcessor;
import com.jay.oss.tracker.processor.MultipartUploadProcessor;
import com.jay.oss.tracker.processor.ObjectProcessor;
import com.jay.oss.tracker.registry.StorageRegistry;
import com.jay.oss.tracker.track.MultipartUploadTracker;
import com.jay.oss.tracker.track.ObjectTracker;

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
                                 EditLogManager editLogManager, MultipartUploadTracker multipartUploadTracker, RecordProducer trackerProducer,
                                 SqlUtil sqlUtil, CommandFactory commandFactory) {
        super(commandFactory);
        BucketProcessor bucketProcessor = new BucketProcessor(bucketManager, storageRegistry, editLogManager,
                objectTracker,sqlUtil, commandFactory);
        ObjectProcessor objectProcessor = new ObjectProcessor(bucketManager, objectTracker, editLogManager, trackerProducer, commandFactory);
        MultipartUploadProcessor multipartUploadProcessor = new MultipartUploadProcessor(bucketManager, objectTracker,  storageRegistry, multipartUploadTracker, editLogManager, commandFactory);
        // 桶相关处理器
        this.registerProcessor(FastOssProtocol.PUT_BUCKET, bucketProcessor);
        this.registerProcessor(FastOssProtocol.LIST_BUCKET, bucketProcessor);
        this.registerProcessor(FastOssProtocol.CHECK_BUCKET_ACL, bucketProcessor);
        this.registerProcessor(FastOssProtocol.BUCKET_PUT_OBJECT, bucketProcessor);
        this.registerProcessor(FastOssProtocol.BUCKET_DELETE_OBJECT, bucketProcessor);
        this.registerProcessor(FastOssProtocol.GET_SERVICE, bucketProcessor);

        // object相关处理器
        this.registerProcessor(FastOssProtocol.LOCATE_OBJECT, objectProcessor);
        this.registerProcessor(FastOssProtocol.DELETE_OBJECT, objectProcessor);
        // MultipartUpload 相关处理器
        this.registerProcessor(FastOssProtocol.INIT_MULTIPART_UPLOAD, multipartUploadProcessor);
        this.registerProcessor(FastOssProtocol.LOOKUP_MULTIPART_UPLOAD, multipartUploadProcessor);
        this.registerProcessor(FastOssProtocol.COMPLETE_MULTIPART_UPLOAD, multipartUploadProcessor);
    }
}
