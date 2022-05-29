package com.jay.oss.tracker.remoting;

import com.jay.dove.transport.command.AbstractCommandHandler;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.oss.common.kafka.RecordProducer;
import com.jay.oss.common.registry.simple.SimpleRegistry;
import com.jay.oss.common.remoting.TinyOssProtocol;
import com.jay.oss.tracker.meta.BucketManager;
import com.jay.oss.tracker.processor.BucketProcessor;
import com.jay.oss.tracker.processor.ObjectProcessor;
import com.jay.oss.tracker.processor.PutObjectMetaProcessor;
import com.jay.oss.tracker.processor.SimpleRegistryProcessor;
import com.jay.oss.tracker.registry.StorageNodeRegistry;
import com.jay.oss.tracker.task.StorageTaskManager;
import com.jay.oss.tracker.track.ObjectTracker;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 *  Tracker端命令分发器
 * </p>
 *
 * @author Jay
 * @date 2022/02/21 10:12
 */
@Slf4j
public class TrackerCommandHandler extends AbstractCommandHandler {
    private final SimpleRegistry simpleRegistry;
    private final ObjectTracker objectTracker;
    public TrackerCommandHandler(BucketManager bucketManager, ObjectTracker objectTracker, StorageNodeRegistry storageRegistry,
                                 RecordProducer trackerProducer, SimpleRegistry simpleRegistry, StorageTaskManager storageTaskManager,
                                 CommandFactory commandFactory) {
        super(commandFactory);
        this.simpleRegistry = simpleRegistry;
        this.objectTracker = objectTracker;
        BucketProcessor bucketProcessor = new BucketProcessor(bucketManager, commandFactory);
        ObjectProcessor objectProcessor = new ObjectProcessor(bucketManager, objectTracker, trackerProducer, commandFactory, storageTaskManager);
        PutObjectMetaProcessor putObjectMetaProcessor = new PutObjectMetaProcessor(commandFactory, bucketManager, storageRegistry, objectTracker);
        SimpleRegistryProcessor simpleRegistryProcessor = new SimpleRegistryProcessor(simpleRegistry, storageTaskManager, objectTracker,commandFactory);
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
        this.registerProcessor(TinyOssProtocol.UPLOAD_COMPLETE, objectProcessor);

        this.registerProcessor(TinyOssProtocol.REGISTER_STORAGE, simpleRegistryProcessor);
        this.registerProcessor(TinyOssProtocol.STORAGE_HEART_BEAT, simpleRegistryProcessor);
    }

    @Override
    public void channelInactive(ChannelHandlerContext channelHandlerContext) {
        Attribute<String> attr = channelHandlerContext.channel().attr(SimpleRegistry.STORAGE_NODE_ATTR);
        if(attr.get() != null && simpleRegistry != null){
            simpleRegistry.setStorageNodeOffline(attr.get());
            objectTracker.onStorageNodeOffline(attr.get());
            log.info("Storage node offline: {}", attr.get());
        }
    }
}
