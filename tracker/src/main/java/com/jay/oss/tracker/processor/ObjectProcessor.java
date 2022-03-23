package com.jay.oss.tracker.processor;

import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.constant.OssConstants;
import com.jay.oss.common.edit.EditLog;
import com.jay.oss.common.edit.EditLogManager;
import com.jay.oss.common.edit.EditOperation;
import com.jay.oss.common.entity.DeleteObjectInBucketRequest;
import com.jay.oss.common.entity.LocateObjectRequest;
import com.jay.oss.common.entity.object.ObjectMeta;
import com.jay.oss.common.entity.object.ObjectVO;
import com.jay.oss.common.kafka.RecordProducer;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.common.util.StringUtil;
import com.jay.oss.tracker.meta.BucketManager;
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
    private final EditLogManager editLogManager;
    private final RecordProducer trackerProducer;

    public ObjectProcessor(BucketManager bucketManager, ObjectTracker objectTracker, EditLogManager editLogManager, RecordProducer trackerProducer, CommandFactory commandFactory) {
        super(commandFactory, bucketManager);
        this.objectTracker = objectTracker;
        this.editLogManager = editLogManager;
        this.trackerProducer = trackerProducer;
    }

    @Override
    public RemotingCommand doProcess(FastOssCommand command) {
        CommandCode code = command.getCommandCode();
        if(FastOssProtocol.LOCATE_OBJECT.equals(code)){
            return locateObject(command);
        }
        else if(FastOssProtocol.DELETE_OBJECT.equals(code)){
            return deleteObject(command);
        }
        else if(FastOssProtocol.GET_OBJECT_META.equals(code)){
            return getObjectMeta(command);
        }
        return commandFactory.createResponse(command.getId(), "", FastOssProtocol.ERROR);
    }

    /**
     * 获取object位置，同时检查存储桶访问权限
     * @param command {@link FastOssCommand}
     * @return {@link RemotingCommand}
     */
    private RemotingCommand locateObject(FastOssCommand command){
        byte[] content = command.getContent();
        LocateObjectRequest request = SerializeUtil.deserialize(content, LocateObjectRequest.class);
        String objectKey = request.getObjectKey();
        // 定位object
        String urls = objectTracker.locateObject(objectKey);
        if(StringUtil.isNullOrEmpty(urls)){
            return commandFactory.createResponse(command.getId(), "", FastOssProtocol.OBJECT_NOT_FOUND);
        }else{
            return commandFactory.createResponse(command.getId(), urls, FastOssProtocol.SUCCESS);
        }
    }

    /**
     * 删除object
     * @param command {@link FastOssCommand}
     * @return {@link RemotingCommand}
     */
    private RemotingCommand deleteObject(FastOssCommand command){
        DeleteObjectInBucketRequest request = SerializeUtil.deserialize(command.getContent(), DeleteObjectInBucketRequest.class);
        String objectKey = request.getObjectKey();
        String bucket = request.getBucket();
        // 定位并删除object
        String urls = objectTracker.locateAndDeleteObject(objectKey);
        if(StringUtil.isNullOrEmpty(urls)){
            return commandFactory.createResponse(command.getId(), "", FastOssProtocol.OBJECT_NOT_FOUND);
        }
        else{
            bucketManager.deleteObject(bucket, objectKey);
            // editLog记录删除操作
            appendDeleteObjectLog(objectKey);
            // 发送删除object消息，由Storage收到消息后异步删除object数据
            trackerProducer.send(OssConstants.DELETE_OBJECT_TOPIC, objectKey, objectKey);
            return commandFactory.createResponse(command.getId(), "", FastOssProtocol.SUCCESS);
        }
    }


    /**
     * 获取object元数据
     * @param command {@link FastOssCommand}
     * @return {@link RemotingCommand}
     */
    private RemotingCommand getObjectMeta(FastOssCommand command){
        LocateObjectRequest request = SerializeUtil.deserialize(command.getContent(), LocateObjectRequest.class);
        String objectKey = request.getObjectKey();
        ObjectMeta objectMeta = objectTracker.getObjectMeta(objectKey);
        if(objectMeta == null){
            return commandFactory.createResponse(command.getId(), "", FastOssProtocol.OBJECT_NOT_FOUND);
        }else{
            byte[] content = SerializeUtil.serialize(getObjectVO(objectMeta), ObjectVO.class);
            return commandFactory.createResponse(command.getId(), content, FastOssProtocol.SUCCESS);
        }
    }


    /**
     * 追加删除object日志
     * @param objectKey objectKey
     */
    private void appendDeleteObjectLog(String objectKey){
        EditLog editLog = new EditLog(EditOperation.BUCKET_DELETE_OBJECT, StringUtil.getBytes(objectKey));
        editLogManager.append(editLog);
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
