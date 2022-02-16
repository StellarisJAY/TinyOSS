package com.jay.oss.storage.processor;

import com.alibaba.fastjson.JSON;
import com.jay.dove.transport.command.AbstractProcessor;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.oss.common.acl.Acl;
import com.jay.oss.common.entity.Bucket;
import com.jay.oss.common.entity.FileMeta;
import com.jay.oss.common.entity.ListBucketRequest;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.util.AccessTokenUtil;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.storage.meta.BucketManager;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * <p>
 *  bucket请求处理器
 * </p>
 *
 * @author Jay
 * @date 2022/02/16 11:37
 */
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
        // 获取bucket
        Bucket bucket = bucketManager.getBucket(request.getBucket());

        FastOssCommand response;
        // bucket 不存在
        if(bucket == null){
            response = (FastOssCommand)commandFactory
                    .createResponse(command.getId(), "bucket not found", FastOssProtocol.NOT_FOUND);
        }else{
            // 检查ACL，
            Acl acl = bucket.getAcl();
            // 如果访问权限是 PRIVATE 且 token无效，拒绝访问
            if(acl == Acl.PRIVATE && !AccessTokenUtil.checkAccessToken(bucket.getAccessKey(), bucket.getSecretKey(), request.getToken())){
                response = (FastOssCommand) commandFactory
                        .createResponse(command.getId(), "invalid access token", FastOssProtocol.ACCESS_DENIED);
            }else{
                // list bucket
                List<FileMeta> objects = bucketManager.listBucket(request.getBucket(), request.getCount(), request.getOffset());
                // 转换成JSON
                String json = JSON.toJSONString(objects);
                response = (FastOssCommand)commandFactory
                        .createResponse(command.getId(), json, FastOssProtocol.SUCCESS);
            }
        }
        sendResponse(context, response);
    }

}
