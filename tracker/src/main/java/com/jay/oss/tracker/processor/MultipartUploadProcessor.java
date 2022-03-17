package com.jay.oss.tracker.processor;

import com.jay.dove.transport.command.AbstractProcessor;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.acl.BucketAccessMode;
import com.jay.oss.common.bitcask.Index;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.edit.EditLog;
import com.jay.oss.common.edit.EditLogManager;
import com.jay.oss.common.edit.EditOperation;
import com.jay.oss.common.entity.BucketPutObjectRequest;
import com.jay.oss.common.entity.CompleteMultipartUploadRequest;
import com.jay.oss.common.entity.LookupMultipartUploadRequest;
import com.jay.oss.common.entity.MultipartUploadTask;
import com.jay.oss.common.entity.object.ObjectMeta;
import com.jay.oss.common.registry.StorageNodeInfo;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.util.KeyUtil;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.tracker.meta.BucketManager;
import com.jay.oss.tracker.registry.StorageRegistry;
import com.jay.oss.tracker.track.MultipartUploadTracker;
import com.jay.oss.tracker.track.ObjectTracker;
import com.jay.oss.tracker.util.BucketAclUtil;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.UUID;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/03/07 14:38
 */
@Slf4j
public class MultipartUploadProcessor extends AbstractProcessor {

    private final BucketManager bucketManager;
    private final ObjectTracker objectTracker;
    private final StorageRegistry storageRegistry;
    private final MultipartUploadTracker uploadTracker;
    private final EditLogManager editLogManager;
    private final CommandFactory commandFactory;

    public MultipartUploadProcessor(BucketManager bucketManager, ObjectTracker objectTracker, StorageRegistry storageRegistry, MultipartUploadTracker uploadTracker, EditLogManager editLogManager, CommandFactory commandFactory) {
        this.bucketManager = bucketManager;
        this.objectTracker = objectTracker;
        this.storageRegistry = storageRegistry;
        this.uploadTracker = uploadTracker;
        this.editLogManager = editLogManager;
        this.commandFactory = commandFactory;
    }

    @Override
    public void process(ChannelHandlerContext channelHandlerContext, Object o) {
        if(o instanceof FastOssCommand){
            FastOssCommand command = (FastOssCommand) o;
            CommandCode code = command.getCommandCode();
            if(FastOssProtocol.INIT_MULTIPART_UPLOAD.equals(code)){
                initMultipartUpload(channelHandlerContext, command);
            }else if(FastOssProtocol.LOOKUP_MULTIPART_UPLOAD.equals(code)){
                lookupMultipartUpload(channelHandlerContext, command);
            }
            else if(FastOssProtocol.COMPLETE_MULTIPART_UPLOAD.equals(code)){
                completeMultipartUpload(channelHandlerContext, command);
            }
        }
    }

    private void initMultipartUpload(ChannelHandlerContext context, FastOssCommand command){
        byte[] content = command.getContent();
        BucketPutObjectRequest request = SerializeUtil.deserialize(content, BucketPutObjectRequest.class);
        String bucket = request.getBucket();
        String token = request.getToken();
        // 无版本号key
        String objectKey = request.getKey();
        RemotingCommand response;
        // 检查存储桶访问权限
        CommandCode code = BucketAclUtil.checkAuthorization(bucketManager, bucket, token, BucketAccessMode.WRITE);
        if(FastOssProtocol.SUCCESS.equals(code)){
            try{
                // 为对象选择storage节点
                List<StorageNodeInfo> nodes = storageRegistry.selectUploadNode(objectKey, request.getSize(), OssConfigs.replicaCount());
                // 记录object位置
                String location = nodesToUrls(nodes);
                String versionId = "";
                if(bucketManager.getBucket(bucket).isVersioning()){
                    versionId = UUID.randomUUID().toString();
                    objectKey  = KeyUtil.appendVersion(objectKey, versionId);
                }
                // 生成上传ID
                String uploadId = UUID.randomUUID().toString();
                // 记录分片上传任务
                MultipartUploadTask task = new MultipartUploadTask(uploadId, objectKey, location, versionId, request.getCreateTime());
                uploadTracker.saveUploadTask(uploadId, task);
                // 记录EditLog
                appendInitMultipartUploadLog(objectKey, task);
                // 返回uploadID
                response = commandFactory.createResponse(command.getId(), uploadId+";"+versionId, FastOssProtocol.SUCCESS);

            }catch (Exception e){
                response = commandFactory.createResponse(command.getId(), "", FastOssProtocol.NO_ENOUGH_STORAGES);
            }
        }else{
            response = commandFactory.createResponse(command.getId(), "", code);
        }
        sendResponse(context, response);
    }


    /**
     * 查询分片上传任务
     * @param context context
     * @param command {@link FastOssCommand}
     */
    private void lookupMultipartUpload(ChannelHandlerContext context, FastOssCommand command){
        LookupMultipartUploadRequest request = SerializeUtil.deserialize(command.getContent(),
                LookupMultipartUploadRequest.class);
        String uploadId = request.getUploadId();
        String objectKey = request.getObjectKey();
        String token = request.getToken();
        String bucket = request.getBucket();
        RemotingCommand response;

        // 检查访问权限
        CommandCode code = BucketAclUtil.checkAuthorization(bucketManager, bucket, token, BucketAccessMode.WRITE);
        if(code.equals(FastOssProtocol.SUCCESS)){
            MultipartUploadTask task = uploadTracker.getTask(uploadId);
            if(task == null){
                // task为null，上传任务无效
                response = commandFactory.createResponse(command.getId(), "", FastOssProtocol.MULTIPART_UPLOAD_FINISHED);
            }else{
                // 检查objectKey是否和上传任务相同
                if(!task.getObjectKey().equals(objectKey)){
                    response = commandFactory.createResponse(command.getId(), "", FastOssProtocol.ERROR);
                }else{
                    response = commandFactory.createResponse(command.getId(), task.getLocations(), FastOssProtocol.SUCCESS);
                }
            }
        }else{
            // 存储桶拒绝访问
            response = commandFactory.createResponse(command.getId(), "", code);
        }
        sendResponse(context, response);
    }

    private void completeMultipartUpload(ChannelHandlerContext context, FastOssCommand command){
        CompleteMultipartUploadRequest request = SerializeUtil.deserialize(command.getContent(), CompleteMultipartUploadRequest.class);
        String bucket = request.getBucket();
        String token = request.getToken();
        String objectKey = request.getObjectKey();
        String uploadId = request.getUploadId();

        // 检查存储桶访问权限
        CommandCode code = BucketAclUtil.checkAuthorization(bucketManager, bucket, token, BucketAccessMode.WRITE);
        RemotingCommand response;
        if(FastOssProtocol.SUCCESS.equals(code)){
            // 获取上传任务记录
            MultipartUploadTask task = uploadTracker.getTask(uploadId);
            if(task == null || !task.getObjectKey().equals(objectKey) || !task.getVersionId().equals(request.getVersionId())){
                // 记录不存在 或者 记录的object和当前object不符
                response = commandFactory.createResponse(command.getId(), "", FastOssProtocol.MULTIPART_UPLOAD_FINISHED);
            }
            else{
                ObjectMeta objectMeta = ObjectMeta.builder()
                        .md5(request.getMd5()).size((long)request.getSize())
                        .locations(task.getLocations()).objectKey(request.getObjectKey())
                        .fileName(request.getFilename()).versionId(request.getVersionId())
                        .createTime(task.getCreateTime())
                        .build();
                uploadTracker.remove(uploadId);
                // 保存object
                objectTracker.putObjectMeta(task.getObjectKey(), objectMeta);
                response = commandFactory.createResponse(command.getId(), task.getLocations(), FastOssProtocol.SUCCESS);
            }
        }
        else{
            response = commandFactory.createResponse(command.getId(), "", code);
        }
        sendResponse(context, response);
    }


    private String nodesToUrls(List<StorageNodeInfo> nodes){
        StringBuilder builder = new StringBuilder();
        for (StorageNodeInfo node : nodes) {
            builder.append(node.getUrl());
            builder.append(";");
        }
        return builder.toString();
    }

    private void appendInitMultipartUploadLog(String objectKey, MultipartUploadTask task){
        byte[] uploadTaskSerialized = SerializeUtil.serialize(task, MultipartUploadTask.class);
        EditLog uploadTaskLog = new EditLog(EditOperation.MULTIPART_UPLOAD, uploadTaskSerialized);
        editLogManager.append(uploadTaskLog);
    }

    private void appendObjectLocationLog(String objectKey){
        Index index = objectTracker.getIndex(objectKey);
        byte[] serialized = SerializeUtil.serialize(index, Index.class);
        EditLog editLog = new EditLog(EditOperation.BUCKET_PUT_OBJECT, serialized);
        editLogManager.append(editLog);
    }
}
