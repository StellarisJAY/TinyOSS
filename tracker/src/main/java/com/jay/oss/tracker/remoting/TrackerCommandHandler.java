package com.jay.oss.tracker.remoting;

import com.jay.dove.transport.command.AbstractCommandHandler;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.oss.common.edit.EditLogManager;
import com.jay.oss.common.kafka.RecordProducer;
import com.jay.oss.common.remoting.TinyOssProtocol;
import com.jay.oss.tracker.meta.BucketManager;
import com.jay.oss.tracker.processor.BucketProcessor;
import com.jay.oss.tracker.processor.MultipartUploadProcessor;
import com.jay.oss.tracker.processor.ObjectProcessor;
import com.jay.oss.tracker.processor.PutObjectMetaProcessor;
import com.jay.oss.tracker.registry.StorageRegistry;
import com.jay.oss.tracker.track.MultipartUploadTracker;
import com.jay.oss.tracker.track.ObjectTracker;

/**
 * <p>
 *  Tracker端命令分发器
 * </p>
 *
 * @author Jay
 * @date 2022/02/21 10:12
 */
public class TrackerCommandHandler extends AbstractCommandHandler {

    public TrackerCommandHandler(BucketManager bucketManager, ObjectTracker objectTracker, StorageRegistry storageRegistry,
                                 EditLogManager editLogManager, MultipartUploadTracker multipartUploadTracker, RecordProducer trackerProducer, CommandFactory commandFactory) {
        super(commandFactory);
        BucketProcessor bucketProcessor = new BucketProcessor(bucketManager, commandFactory);
        ObjectProcessor objectProcessor = new ObjectProcessor(bucketManager, objectTracker, trackerProducer, commandFactory);
        PutObjectMetaProcessor putObjectMetaProcessor = new PutObjectMetaProcessor(commandFactory, bucketManager, storageRegistry, objectTracker);
        MultipartUploadProcessor multipartUploadProcessor = new MultipartUploadProcessor(bucketManager, objectTracker,  storageRegistry, multipartUploadTracker, editLogManager, commandFactory);
        // 桶相关处理器
        this.registerProcessor(TinyOssProtocol.PUT_BUCKET, bucketProcessor);
        this.registerProcessor(TinyOssProtocol.LIST_BUCKET, bucketProcessor);
        this.registerProcessor(TinyOssProtocol.CHECK_BUCKET_ACL, bucketProcessor);
        this.registerProcessor(TinyOssProtocol.BUCKET_DELETE_OBJECT, bucketProcessor);
        this.registerProcessor(TinyOssProtocol.GET_SERVICE, bucketProcessor);
        this.registerProcessor(TinyOssProtocol.UPDATE_BUCKET_ACL, bucketProcessor);

        this.registerProcessor(TinyOssProtocol.BUCKET_PUT_OBJECT, putObjectMetaProcessor);

        // object相关处理器
        this.registerProcessor(TinyOssProtocol.LOCATE_OBJECT, objectProcessor);
        this.registerProcessor(TinyOssProtocol.DELETE_OBJECT, objectProcessor);
        this.registerProcessor(TinyOssProtocol.GET_OBJECT_META, objectProcessor);
        // MultipartUpload 相关处理器
        this.registerProcessor(TinyOssProtocol.INIT_MULTIPART_UPLOAD, multipartUploadProcessor);
        this.registerProcessor(TinyOssProtocol.LOOKUP_MULTIPART_UPLOAD, multipartUploadProcessor);
        this.registerProcessor(TinyOssProtocol.COMPLETE_MULTIPART_UPLOAD, multipartUploadProcessor);
    }
}
