package com.jay.oss.tracker.processor;

import com.jay.dove.transport.command.AbstractProcessor;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.acl.BucketAccessMode;
import com.jay.oss.common.constant.OssConstants;
import com.jay.oss.common.edit.EditLog;
import com.jay.oss.common.edit.EditLogManager;
import com.jay.oss.common.edit.EditOperation;
import com.jay.oss.common.entity.DeleteObjectInBucketRequest;
import com.jay.oss.common.entity.LocateObjectRequest;
import com.jay.oss.common.entity.object.ObjectMeta;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.common.util.StringUtil;
import com.jay.oss.tracker.kafka.TrackerProducer;
import com.jay.oss.tracker.meta.BucketManager;
import com.jay.oss.tracker.track.ObjectTracker;
import com.jay.oss.tracker.util.BucketAclUtil;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

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
public class ObjectProcessor extends AbstractProcessor {
    private final BucketManager bucketManager;
    private final ObjectTracker objectTracker;
    private final EditLogManager editLogManager;
    private final TrackerProducer trackerProducer;
    private final CommandFactory commandFactory;
    //private final KafkaProducer<String, String> deleteMessageProducer;

    public ObjectProcessor(BucketManager bucketManager, ObjectTracker objectTracker, EditLogManager editLogManager, TrackerProducer trackerProducer, CommandFactory commandFactory) {
        this.bucketManager = bucketManager;
        this.objectTracker = objectTracker;
        this.editLogManager = editLogManager;
        this.trackerProducer = trackerProducer;
        this.commandFactory = commandFactory;
        //this.deleteMessageProducer = new KafkaProducer<>(OssConfigs.getProperties(), new StringSerializer(), new StringSerializer());
    }

    @Override
    public void process(ChannelHandlerContext channelHandlerContext, Object o) {
        if(o instanceof FastOssCommand){
            FastOssCommand command = (FastOssCommand) o;
            CommandCode code = command.getCommandCode();
            if(FastOssProtocol.LOCATE_OBJECT.equals(code)){
                locateObject(channelHandlerContext, command);
            }
            else if(FastOssProtocol.DELETE_OBJECT.equals(code)){
                deleteObject(channelHandlerContext, command);
            }
        }
    }

    /**
     * 获取object位置，同时检查存储桶访问权限
     * @param context {@link ChannelHandlerContext}
     * @param command {@link FastOssCommand}
     */
    private void locateObject(ChannelHandlerContext context, FastOssCommand command){
        byte[] content = command.getContent();
        LocateObjectRequest request = SerializeUtil.deserialize(content, LocateObjectRequest.class);
        String bucket = request.getBucket();
        String token = request.getToken();
        String objectKey = request.getObjectKey();
        RemotingCommand response;
        // 检查存储桶访问权限
        CommandCode code = BucketAclUtil.checkAuthorization(bucketManager, bucket, token, BucketAccessMode.READ);
        if(FastOssProtocol.SUCCESS.equals(code)){
            // 定位object
            String urls = objectTracker.locateObject(objectKey);
            if(StringUtil.isNullOrEmpty(urls)){
                response = commandFactory.createResponse(command.getId(), "", FastOssProtocol.OBJECT_NOT_FOUND);
            }else{
                response = commandFactory.createResponse(command.getId(), urls, FastOssProtocol.SUCCESS);
            }
        }else{
            response = commandFactory.createResponse(command.getId(), "", code);
        }
        sendResponse(context, response);
    }

    /**
     * 删除object
     * @param context {@link ChannelHandlerContext}
     * @param command {@link FastOssCommand}
     */
    private void deleteObject(ChannelHandlerContext context, FastOssCommand command){
        DeleteObjectInBucketRequest request = SerializeUtil.deserialize(command.getContent(), DeleteObjectInBucketRequest.class);
        String bucket = request.getBucket();
        String objectKey = request.getObjectKey();
        String token = request.getToken();
        // 检查存储桶权限
        CommandCode code = BucketAclUtil.checkAuthorization(bucketManager, bucket, token, BucketAccessMode.WRITE);
        RemotingCommand response;
        if(FastOssProtocol.SUCCESS.equals(code)){
            // 定位并删除object
            String urls = objectTracker.locateAndDeleteObject(objectKey);
            if(StringUtil.isNullOrEmpty(urls)){
                response = commandFactory.createResponse(command.getId(), "", FastOssProtocol.OBJECT_NOT_FOUND);
            }
            else{
                bucketManager.deleteObject(bucket, objectKey);
                // editLog记录删除操作
                appendDeleteObjectLog(objectKey);
                // 发送删除object消息，由Storage收到消息后异步删除object数据
                Future<RecordMetadata> future = trackerProducer.send(OssConstants.DELETE_OBJECT_TOPIC, objectKey, objectKey);
                response = commandFactory.createResponse(command.getId(), "", FastOssProtocol.SUCCESS);
            }
        }
        else{
            response = commandFactory.createResponse(command.getId(), "", code);
        }
        sendResponse(context, response);
    }


    /**
     * 获取object元数据
     * @param context {@link ChannelHandlerContext}
     * @param command {@link FastOssCommand}
     */
    private void getObjectMeta(ChannelHandlerContext context, FastOssCommand command){
        LocateObjectRequest request = SerializeUtil.deserialize(command.getContent(), LocateObjectRequest.class);
        String bucket = request.getBucket();
        String objectKey = request.getObjectKey();
        String token = request.getToken();
        CommandCode code = BucketAclUtil.checkAuthorization(bucketManager, bucket, token, BucketAccessMode.READ);
        RemotingCommand response;
        if(FastOssProtocol.SUCCESS.equals(code)){
            ObjectMeta objectMeta = objectTracker.getObjectMeta(objectKey);
            if(objectMeta == null){
                response = commandFactory.createResponse(command.getId(), "", FastOssProtocol.OBJECT_NOT_FOUND);
            }else{
                byte[] content = SerializeUtil.serialize(objectMeta, ObjectMeta.class);
                response = commandFactory.createResponse(command.getId(), content, code);
            }
        }else{
            response = commandFactory.createResponse(command.getId(), "", code);
        }
        sendResponse(context, response);
    }



    private void appendDeleteObjectLog(String objectKey){
        EditLog editLog = new EditLog(EditOperation.BUCKET_DELETE_OBJECT, StringUtil.getBytes(objectKey));
        editLogManager.append(editLog);
    }
}
