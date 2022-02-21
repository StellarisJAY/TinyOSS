package com.jay.oss.tracker.processor;

import com.alibaba.fastjson.JSON;
import com.jay.dove.transport.command.AbstractProcessor;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.oss.common.acl.BucketAccessMode;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.entity.*;
import com.jay.oss.common.registry.StorageNodeInfo;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.tracker.meta.BucketManager;
import com.jay.oss.tracker.registry.StorageRegistry;
import com.jay.oss.tracker.util.BucketAclUtil;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/02/21 10:12
 */
public class BucketProcessor extends AbstractProcessor {

    private final BucketManager bucketManager;
    private final StorageRegistry storageRegistry;
    private final CommandFactory commandFactory;
    public BucketProcessor(BucketManager bucketManager, StorageRegistry storageRegistry, CommandFactory commandFactory) {
        this.bucketManager = bucketManager;
        this.commandFactory = commandFactory;
        this.storageRegistry = storageRegistry;
    }
    @Override
    public void process(ChannelHandlerContext context, Object o) {
        if(o instanceof FastOssCommand){
            FastOssCommand command = (FastOssCommand) o;
            CommandCode code = command.getCommandCode();

            if(FastOssProtocol.PUT_BUCKET.equals(code)){
                processPutBucket(context, command);
            }
            else if(FastOssProtocol.LIST_BUCKET.equals(code)){
                processListBucket(context, command);
            }
            else if(FastOssProtocol.CHECK_BUCKET_ACL.equals(code)){
                checkBucketAcl(context, command);
            }
            else if(FastOssProtocol.BUCKET_PUT_OBJECT.equals(code)){
                bucketPutObject(context, command);
            }
            else if(FastOssProtocol.BUCKET_DELETE_OBJECT.equals(code)){
                bucketDeleteObject(context, command);
            }
        }
    }

    /**
     * 处理put bucket请求
     * @param context {@link io.netty.channel.ChannelHandlerContext}
     * @param command {@link FastOssCommand}
     */
    private void processPutBucket(ChannelHandlerContext context, FastOssCommand command){
        byte[] content = command.getContent();
        Bucket bucket = SerializeUtil.deserialize(content, Bucket.class);
        String keyPair = bucketManager.addBucket(bucket);
        FastOssCommand response = (FastOssCommand) commandFactory
                .createResponse(command.getId(), keyPair, FastOssProtocol.SUCCESS);
        sendResponse(context, response);
    }

    /**
     * 处理list bucket请求
     * @param context {@link ChannelHandlerContext}
     * @param command {@link FastOssCommand}
     */
    private void processListBucket(ChannelHandlerContext context, FastOssCommand command){
        byte[] content = command.getContent();
        // 反序列化请求
        ListBucketRequest request = SerializeUtil.deserialize(content, ListBucketRequest.class);
        // 检查存储桶访问权限
        CommandCode code = BucketAclUtil.checkAuthorization(bucketManager, request.getBucket(), request.getToken(), BucketAccessMode.READ);
        FastOssCommand response;
        // 权限通过
        if(code.equals(FastOssProtocol.SUCCESS)){
            // list bucket
            List<FileMeta> objects = bucketManager.listBucket(request.getBucket(), request.getCount(), request.getOffset());
            // 转换成JSON
            String json = JSON.toJSONString(objects);
            response = (FastOssCommand) commandFactory
                    .createResponse(command.getId(), json, code);
        }else{
            response = (FastOssCommand) commandFactory
                    .createResponse(command.getId(), "", code);
        }
        sendResponse(context, response);
    }

    /**
     * 检查桶访问权限
     * @param context context
     * @param command command
     */
    private void checkBucketAcl(ChannelHandlerContext context, FastOssCommand command){
        byte[] content = command.getContent();
        // 反序列化请求
        CheckBucketAclRequest request = SerializeUtil.deserialize(content, CheckBucketAclRequest.class);
        // 检查权限，并返回response
        CommandCode code = BucketAclUtil.checkAuthorization(bucketManager, request.getBucket(), request.getToken(), request.getAccessMode());
        FastOssCommand response = (FastOssCommand)commandFactory.createResponse(command.getId(), "", code);
        // 发送response
        sendResponse(context, response);
    }

    /**
     * 处理向桶中放入object元数据
     * @param context {@link ChannelHandlerContext}
     * @param command {@link FastOssCommand}
     */
    private void bucketPutObject(ChannelHandlerContext context, FastOssCommand command){
        byte[] content = command.getContent();
        // 反序列化请求
        BucketPutObjectRequest request = SerializeUtil.deserialize(content, BucketPutObjectRequest.class);
        String bucket = request.getBucket();
        String token = request.getToken();
        // 检查存储桶访问权限
        CommandCode code = BucketAclUtil
                .checkAuthorization(bucketManager, bucket, token, BucketAccessMode.WRITE);
        FastOssCommand response;
        // 拥有权限，完成put object
        if(code.equals(FastOssProtocol.SUCCESS)){
            // 选择上传点
            List<StorageNodeInfo> nodes = storageRegistry.selectUploadNode(request.getSize(), OssConfigs.replicaCount());

            // 没有可用上传点
            if(nodes == null || nodes.isEmpty()){
                response = (FastOssCommand) commandFactory
                        .createResponse(command.getId(), "no available storages", FastOssProtocol.ACCESS_DENIED);
            }
            else{
                // 创建文件Meta
                FileMeta meta = FileMeta.builder()
                        .key(request.getKey()).createTime(request.getCreateTime())
                        .filename(request.getFilename()).size(request.getSize()).build();
                // 保存到存储桶
                bucketManager.saveMeta(request.getBucket(), meta);
                // 拼接候选url
                StringBuilder builder = new StringBuilder();
                for (StorageNodeInfo node : nodes) {
                    builder.append(node.getUrl());
                    builder.append(";");
                }
                response = (FastOssCommand) commandFactory.createResponse(command.getId(), builder.toString(), code);
            }
        }else{
            // 没有访问权限 或者 存储桶不存在
            response = (FastOssCommand) commandFactory.createResponse(command.getId(), "", code);
        }
        // 发送结果
        sendResponse(context, response);
    }

    /**
     * 删除存储桶内的object记录
     * @param context {@link ChannelHandlerContext}
     * @param command {@link FastOssCommand}
     */
    private void bucketDeleteObject(ChannelHandlerContext context, FastOssCommand command){
        byte[] content = command.getContent();
        //  反序列化请求
        DeleteObjectInBucketRequest request = SerializeUtil.deserialize(content, DeleteObjectInBucketRequest.class);
        // 检查存储桶访问权限
        CommandCode code = BucketAclUtil.checkAuthorization(bucketManager, request.getBucket(), request.getBucket(), BucketAccessMode.WRITE);
        FastOssCommand response;
        // 权限通过
        if(code.equals(FastOssProtocol.SUCCESS)){
            // 删除object记录
            boolean delete = bucketManager.deleteMeta(request.getBucket(), request.getKey());
            if(!delete){
                // 删除失败，object不存在
                response = (FastOssCommand) commandFactory
                        .createResponse(command.getId(), "", FastOssProtocol.NOT_FOUND);
            }else{
                response = (FastOssCommand) commandFactory
                        .createResponse(command.getId(), "", FastOssProtocol.SUCCESS);
            }
        }else{
            response = (FastOssCommand) commandFactory
                    .createResponse(command.getId(), "", code);
        }
        sendResponse(context, response);
    }
}
