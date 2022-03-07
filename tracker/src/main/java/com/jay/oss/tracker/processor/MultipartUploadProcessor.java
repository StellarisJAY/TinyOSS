package com.jay.oss.tracker.processor;

import com.jay.dove.transport.command.AbstractProcessor;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.oss.common.acl.BucketAccessMode;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.entity.BucketPutObjectRequest;
import com.jay.oss.common.registry.StorageNodeInfo;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.tracker.meta.BucketManager;
import com.jay.oss.tracker.registry.StorageRegistry;
import com.jay.oss.tracker.track.MultipartUploadTracker;
import com.jay.oss.tracker.track.ObjectTracker;
import com.jay.oss.tracker.util.BucketAclUtil;
import io.netty.channel.ChannelHandlerContext;

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
public class MultipartUploadProcessor extends AbstractProcessor {

    private final BucketManager bucketManager;
    private final ObjectTracker objectTracker;
    private final StorageRegistry storageRegistry;
    private final MultipartUploadTracker uploadTracker;
    private final CommandFactory commandFactory;

    public MultipartUploadProcessor(BucketManager bucketManager, ObjectTracker objectTracker, StorageRegistry storageRegistry, MultipartUploadTracker uploadTracker, CommandFactory commandFactory) {
        this.bucketManager = bucketManager;
        this.objectTracker = objectTracker;
        this.storageRegistry = storageRegistry;
        this.uploadTracker = uploadTracker;
        this.commandFactory = commandFactory;
    }

    @Override
    public void process(ChannelHandlerContext channelHandlerContext, Object o) {
        if(o instanceof FastOssCommand){
            FastOssCommand command = (FastOssCommand) o;
            CommandCode code = command.getCommandCode();
            if(FastOssProtocol.INIT_MULTIPART_UPLOAD.equals(code)){
                initMultipartUpload(channelHandlerContext, command);
            }
        }
    }

    private void initMultipartUpload(ChannelHandlerContext context, FastOssCommand command){
        byte[] content = command.getContent();
        BucketPutObjectRequest request = SerializeUtil.deserialize(content, BucketPutObjectRequest.class);
        String bucket = request.getBucket();
        String token = request.getToken();
        String objectKey = request.getKey();
        FastOssCommand response;
        // 检查存储桶访问权限
        CommandCode code = BucketAclUtil.checkAuthorization(bucketManager, bucket, token, BucketAccessMode.WRITE);
        if(FastOssProtocol.SUCCESS.equals(code)){
            try{
                // 为对象选择storage节点
                List<StorageNodeInfo> nodes = storageRegistry.selectUploadNode(objectKey, request.getSize(), OssConfigs.replicaCount());
                // 生成上传ID
                String uploadId = UUID.randomUUID().toString();
                // 记录object位置
                String location = nodesToUrls(nodes);
                objectTracker.saveObjectLocation(objectKey, location);
                uploadTracker.saveUploadLocation(uploadId, location);
                // 返回uploadID
                response = (FastOssCommand) commandFactory.createResponse(command.getId(), uploadId, FastOssProtocol.SUCCESS);
            }catch (Exception e){
                response = (FastOssCommand) commandFactory.createResponse(command.getId(), "", FastOssProtocol.NO_ENOUGH_STORAGES);
            }
        }else{
            response = (FastOssCommand) commandFactory.createResponse(command.getId(), "", code);
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
}
