package com.jay.oss.tracker.processor;

import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.bitcask.HintIndex;
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
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.UUID;

/**
 * <p>
 *  Multipart Upload 处理器
 * </p>
 *
 * @author Jay
 * @date 2022/03/07 14:38
 */
@Slf4j
public class MultipartUploadProcessor extends TrackerProcessor {
    private final ObjectTracker objectTracker;
    private final StorageRegistry storageRegistry;
    private final MultipartUploadTracker uploadTracker;
    private final EditLogManager editLogManager;

    public MultipartUploadProcessor(BucketManager bucketManager, ObjectTracker objectTracker, StorageRegistry storageRegistry, MultipartUploadTracker uploadTracker, EditLogManager editLogManager, CommandFactory commandFactory) {
        super(commandFactory, bucketManager);
        this.objectTracker = objectTracker;
        this.storageRegistry = storageRegistry;
        this.uploadTracker = uploadTracker;
        this.editLogManager = editLogManager;
    }

    @Override
    public RemotingCommand doProcess(FastOssCommand command) {
        CommandCode code = command.getCommandCode();
        if(FastOssProtocol.INIT_MULTIPART_UPLOAD.equals(code)){
            return initMultipartUpload(command);
        }else if(FastOssProtocol.LOOKUP_MULTIPART_UPLOAD.equals(code)){
            return lookupMultipartUpload(command);
        }
        else if(FastOssProtocol.COMPLETE_MULTIPART_UPLOAD.equals(code)){
            return completeMultipartUpload(command);
        }
        return null;
    }

    /**
     * 处理初始化MultipartUpload
     * @param command {@link FastOssCommand}
     * @return {@link RemotingCommand}
     */
    private RemotingCommand initMultipartUpload(FastOssCommand command){
        byte[] content = command.getContent();
        BucketPutObjectRequest request = SerializeUtil.deserialize(content, BucketPutObjectRequest.class);
        String bucket = request.getBucket();
        // 无版本号key
        String objectKey = request.getKey();
        RemotingCommand response;
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
            if(uploadTracker.saveUploadTask(uploadId, task)){
                // 记录EditLog
                appendInitMultipartUploadLog(uploadId);
                // 返回uploadID
                response = commandFactory.createResponse(command.getId(), uploadId+";"+versionId, FastOssProtocol.SUCCESS);
            }
            else{
                response = commandFactory.createResponse(command.getId(), "", FastOssProtocol.ERROR);
            }

        }catch (Exception e){
            response = commandFactory.createResponse(command.getId(), "", FastOssProtocol.NO_ENOUGH_STORAGES);
        }
        return response;
    }




    /**
     * 查询分片上传任务
     * @param command {@link FastOssCommand}
     * @return {@link RemotingCommand}
     */
    private RemotingCommand lookupMultipartUpload(FastOssCommand command){
        LookupMultipartUploadRequest request = SerializeUtil.deserialize(command.getContent(),
                LookupMultipartUploadRequest.class);
        String uploadId = request.getUploadId();
        String objectKey = request.getObjectKey();
        MultipartUploadTask task = uploadTracker.getTask(uploadId);
        if(task == null){
            // task为null，上传任务无效
            return commandFactory.createResponse(command.getId(), "", FastOssProtocol.MULTIPART_UPLOAD_FINISHED);
        }else{
            // 检查objectKey是否和上传任务相同
            if(!task.getObjectKey().equals(objectKey)){
                return commandFactory.createResponse(command.getId(), "", FastOssProtocol.ERROR);
            }else{
                return commandFactory.createResponse(command.getId(), task.getLocations(), FastOssProtocol.SUCCESS);
            }
        }
    }


    /**
     * 完成MultipartUpload
     * 将元数据保存，并删除upload记录
     * @param command {@link FastOssCommand}
     * @return {@link RemotingCommand}
     */
    private RemotingCommand completeMultipartUpload(FastOssCommand command){
        CompleteMultipartUploadRequest request = SerializeUtil.deserialize(command.getContent(), CompleteMultipartUploadRequest.class);
        String bucket = request.getBucket();
        String objectKey = request.getObjectKey();
        String uploadId = request.getUploadId();
        RemotingCommand response;
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
            // 保存object,检查key是否重复
            if(objectTracker.putObjectMeta(task.getObjectKey(), objectMeta)){
                appendBucketPutObjectLog(task.getObjectKey());
                bucketManager.putObject(bucket, objectMeta.getObjectKey());
                response = commandFactory.createResponse(command.getId(), task.getLocations(), FastOssProtocol.SUCCESS);
            }else{
                // object key 重复，返回locations，让客户端通知storage删除分片
                response = commandFactory.createResponse(command.getId(), task.getUploadId(), FastOssProtocol.DUPLICATE_OBJECT_KEY);
            }
        }
        return response;
    }


    /**
     * 从Node节点信息得出url字符串
     * @param nodes {@link StorageNodeInfo}
     * @return String
     */
    private String nodesToUrls(List<StorageNodeInfo> nodes){
        StringBuilder builder = new StringBuilder();
        for (StorageNodeInfo node : nodes) {
            builder.append(node.getUrl());
            builder.append(";");
        }
        return builder.toString();
    }


    /**
     * 添加初始化MultipartUpload日志
     * @param uploadId 上传任务ID
     */
    private void appendInitMultipartUploadLog(String uploadId){
        Index index = uploadTracker.getIndex(uploadId);
        HintIndex hint = new HintIndex(uploadId, index.getChunkId(), index.getOffset(), index.isRemoved());
        byte[] content = SerializeUtil.serialize(hint, HintIndex.class);
        EditLog editLog = new EditLog(EditOperation.MULTIPART_UPLOAD, content);
        editLogManager.append(editLog);
    }

    /**
     * 添加存储同PutObject日志
     * @param objectKey objectKey
     */
    private void appendBucketPutObjectLog(String objectKey){
        Index index = objectTracker.getIndex(objectKey);
        HintIndex hint = new HintIndex(objectKey, index.getChunkId(), index.getOffset(), index.isRemoved());
        byte[] serialized = SerializeUtil.serialize(hint, HintIndex.class);
        EditLog editLog = new EditLog(EditOperation.BUCKET_PUT_OBJECT, serialized);
        editLogManager.append(editLog);
    }
}
