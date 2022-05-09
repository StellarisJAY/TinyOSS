package com.jay.oss.tracker.processor;

import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.constant.OssConstants;
import com.jay.oss.common.entity.object.ObjectMeta;
import com.jay.oss.common.entity.object.ObjectVO;
import com.jay.oss.common.entity.request.DeleteObjectInBucketRequest;
import com.jay.oss.common.entity.request.LocateObjectRequest;
import com.jay.oss.common.entity.task.DeleteTask;
import com.jay.oss.common.kafka.RecordProducer;
import com.jay.oss.common.remoting.TinyOssCommand;
import com.jay.oss.common.remoting.TinyOssProtocol;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.common.util.StringUtil;
import com.jay.oss.tracker.meta.BucketManager;
import com.jay.oss.tracker.task.StorageTaskManager;
import com.jay.oss.tracker.track.ObjectTracker;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 *  Object 处理器
 *  处理object 定位
 * </p>
 *
 * @author Jay
 * @date 2022/03/02 12:07
 */
@Slf4j
public class ObjectProcessor extends TrackerProcessor {
    private final ObjectTracker objectTracker;
    private final RecordProducer trackerProducer;
    private final StorageTaskManager storageTaskManager;

    public ObjectProcessor(BucketManager bucketManager, ObjectTracker objectTracker, RecordProducer trackerProducer, CommandFactory commandFactory, StorageTaskManager storageTaskManager) {
        super(commandFactory, bucketManager);
        this.objectTracker = objectTracker;
        this.trackerProducer = trackerProducer;
        this.storageTaskManager = storageTaskManager;
    }

    @Override
    public RemotingCommand doProcess(TinyOssCommand command) {
        CommandCode code = command.getCommandCode();
        if(TinyOssProtocol.LOCATE_OBJECT.equals(code)){
            return locateObject(command);
        }
        else if(TinyOssProtocol.DELETE_OBJECT.equals(code)){
            return deleteObject(command);
        }
        else if(TinyOssProtocol.GET_OBJECT_META.equals(code)){
            return getObjectMeta(command);
        }
        return commandFactory.createResponse(command.getId(), "", TinyOssProtocol.ERROR);
    }

    /**
     * 获取object位置，同时检查存储桶访问权限
     * @param command {@link TinyOssCommand}
     * @return {@link RemotingCommand}
     */
    private RemotingCommand locateObject(TinyOssCommand command){
        byte[] content = command.getContent();
        LocateObjectRequest request = SerializeUtil.deserialize(content, LocateObjectRequest.class);
        String objectKey = request.getObjectKey();
        // 定位object
        String urls = objectTracker.locateObject(objectKey);
        if(StringUtil.isNullOrEmpty(urls)){
            return commandFactory.createResponse(command.getId(), "", TinyOssProtocol.OBJECT_NOT_FOUND);
        }else{
            return commandFactory.createResponse(command.getId(), urls, TinyOssProtocol.SUCCESS);
        }
    }

    /**
     * 删除object
     * @param command {@link TinyOssCommand}
     * @return {@link RemotingCommand}
     */
    private RemotingCommand deleteObject(TinyOssCommand command){
        DeleteObjectInBucketRequest request = SerializeUtil.deserialize(command.getContent(), DeleteObjectInBucketRequest.class);
        String objectKey = request.getObjectKey();
        // 定位并删除object
        ObjectMeta meta = objectTracker.deleteObjectMeta(objectKey);
        if(meta == null){
            return commandFactory.createResponse(command.getId(), "", TinyOssProtocol.OBJECT_NOT_FOUND);
        }
        if(OssConfigs.enableTrackerMessaging()){
            String[] storages = meta.getLocations().split(";");
            DeleteTask task = new DeleteTask(0L, meta.getObjectId());
            for (String storage : storages) {
                storageTaskManager.addDeleteTask(storage, task);
            }
        }else{
            // 发送删除object消息，由Storage收到消息后异步删除object数据
            trackerProducer.send(OssConstants.DELETE_OBJECT_TOPIC, Long.toString(meta.getObjectId()), Long.toString(meta.getObjectId()));
        }
        return commandFactory.createResponse(command.getId(), "", TinyOssProtocol.SUCCESS);
    }


    /**
     * 获取object元数据
     * @param command {@link TinyOssCommand}
     * @return {@link RemotingCommand}
     */
    private RemotingCommand getObjectMeta(TinyOssCommand command){
        LocateObjectRequest request = SerializeUtil.deserialize(command.getContent(), LocateObjectRequest.class);
        String objectKey = request.getObjectKey();
        ObjectMeta objectMeta = objectTracker.getObjectMeta(objectKey);
        if(objectMeta == null){
            return commandFactory.createResponse(command.getId(), "", TinyOssProtocol.OBJECT_NOT_FOUND);
        }else{
            byte[] content = SerializeUtil.serialize(getObjectVO(objectMeta), ObjectVO.class);
            return commandFactory.createResponse(command.getId(), content, TinyOssProtocol.SUCCESS);
        }
    }

    /**
     * object元数据转换成VO
     * @param meta {@link ObjectMeta}
     * @return {@link ObjectVO}
     */
    private ObjectVO getObjectVO(ObjectMeta meta){
        return ObjectVO.builder()
                .objectKey(meta.getObjectKey()).fileName(meta.getFileName())
                .createTime(meta.getCreateTime()).md5(meta.getMd5())
                .versionId(meta.getVersionId())
                .size(meta.getSize())
                .build();
    }
}
