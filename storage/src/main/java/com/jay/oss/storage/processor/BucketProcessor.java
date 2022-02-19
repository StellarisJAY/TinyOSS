package com.jay.oss.storage.processor;

import com.alibaba.fastjson.JSON;
import com.jay.dove.transport.command.AbstractProcessor;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.oss.common.acl.Acl;
import com.jay.oss.common.acl.BucketAccessMode;
import com.jay.oss.common.entity.*;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.util.AccessTokenUtil;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.storage.meta.BucketManager;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * <p>
 *  bucket请求处理器
 * </p>
 *
 * @author Jay
 * @date 2022/02/16 11:37
 */
@Slf4j
public class BucketProcessor extends AbstractProcessor {

    private final BucketManager bucketManager;
    private final CommandFactory commandFactory;
    public BucketProcessor(BucketManager bucketManager, CommandFactory commandFactory) {
        this.bucketManager = bucketManager;
        this.commandFactory = commandFactory;
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
        // 检查访问权限
        FastOssCommand response = checkAuthorization(request.getBucket(), request.getToken(), BucketAccessMode.READ, command.getId());
        // 权限通过
        if(response.getCommandCode().equals(FastOssProtocol.SUCCESS)){
            // list bucket
            List<FileMeta> objects = bucketManager.listBucket(request.getBucket(), request.getCount(), request.getOffset());
            // 转换成JSON
            String json = JSON.toJSONString(objects);
            byte[] jsonBytes = json.getBytes(StandardCharsets.UTF_8);
            response.setContent(jsonBytes);
            response.setLength(FastOssProtocol.HEADER_LENGTH + jsonBytes.length);
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
        FastOssCommand response = checkAuthorization(request.getBucket(), request.getToken(), request.getAccessMode(), command.getId());
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
        BucketPutObjectRequest request = SerializeUtil.deserialize(content, BucketPutObjectRequest.class);

        // 检查访问权限，并创建返回报文
        FastOssCommand response = checkAuthorization(request.getBucket(), request.getToken(), BucketAccessMode.WRITE, command.getId());
        // 拥有权限，完成put object
        if(response.getCommandCode().equals(FastOssProtocol.SUCCESS)){
            // 创建文件Meta
            FileMeta meta = FileMeta.builder()
                    .key(request.getKey()).createTime(request.getCreateTime())
                    .filename(request.getFilename()).size(request.getSize()).build();
            bucketManager.saveMeta(request.getBucket(), meta);
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
        // 检查访问权限
        FastOssCommand response = checkAuthorization(request.getBucket(), request.getToken(), BucketAccessMode.WRITE, command.getId());
        // 权限通过
        if(response.getCommandCode().equals(FastOssProtocol.SUCCESS)){
            // 删除object记录
            boolean delete = bucketManager.deleteMeta(request.getBucket(), request.getKey());
            if(!delete){
                // 删除失败，object不存在
                response.setCommandCode(FastOssProtocol.NOT_FOUND);
            }
        }
        sendResponse(context, response);
    }

    /**
     * 检查访问权限，并创建回复报文
     * @param bucketName 桶
     * @param token token
     * @param accessMode 访问模式，{@link BucketAccessMode}
     * @param commandId 请求ID
     * @return {@link FastOssCommand} 回复报文
     */
    private FastOssCommand checkAuthorization(String bucketName, String token, BucketAccessMode accessMode, int commandId){
        // 获取桶
        Bucket bucket = bucketManager.getBucket(bucketName);
        FastOssCommand command;
        // 桶是否存在
        if(bucket != null){
            String accessKey = bucket.getAccessKey();
            String secretKey = bucket.getSecretKey();
            Acl acl = bucket.getAcl();
            // 检查 READ 权限
            if(accessMode == BucketAccessMode.READ){
                // PRIVATE acl下需要检查token
                if(acl == Acl.PRIVATE && !AccessTokenUtil.checkAccessToken(accessKey, secretKey, token)){
                    command = (FastOssCommand)commandFactory
                            .createResponse(commandId, "ACCESS DENIED", FastOssProtocol.ACCESS_DENIED);
                }else{
                    command = (FastOssCommand)commandFactory
                            .createResponse(commandId, "SUCCESS", FastOssProtocol.SUCCESS);
                }
            }
            // 检查 WRITE权限
            else if(accessMode == BucketAccessMode.WRITE){
                // 非PUBLIC_WRITE acl下需要检验token
                if(acl != Acl.PUBLIC_WRITE && !AccessTokenUtil.checkAccessToken(accessKey, secretKey, token)){
                    command = (FastOssCommand)commandFactory
                            .createResponse(commandId, "ACCESS DENIED", FastOssProtocol.ACCESS_DENIED);
                }else{
                    command = (FastOssCommand)commandFactory
                            .createResponse(commandId, "SUCCESS", FastOssProtocol.SUCCESS);
                }
            }
            // 检查WRITE_ACL权限
            else{
                if(!AccessTokenUtil.checkAccessToken(accessKey, secretKey, token)){
                    command = (FastOssCommand)commandFactory
                            .createResponse(commandId, "ACCESS DENIED", FastOssProtocol.ACCESS_DENIED);
                }else{
                    command = (FastOssCommand)commandFactory
                            .createResponse(commandId, "SUCCESS", FastOssProtocol.SUCCESS);
                }
            }

        }else{
            // 桶不存在，返回NOT_FOUND
            command = (FastOssCommand)commandFactory
                    .createResponse(commandId, "NOT FOUND", FastOssProtocol.NOT_FOUND);
        }
        return command;
    }

}
