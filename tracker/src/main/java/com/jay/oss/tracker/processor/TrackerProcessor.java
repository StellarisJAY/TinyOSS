package com.jay.oss.tracker.processor;

import com.jay.dove.transport.command.AbstractProcessor;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.entity.*;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.tracker.meta.BucketManager;
import com.jay.oss.tracker.util.BucketAclUtil;
import io.netty.channel.ChannelHandlerContext;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 *  Tracker端Processor
 *  继承该Processor并实现doProcess方法。
 *  该类的主要作用是检查存储桶访问权限，将权限检查代码和业务代码解耦
 * </p>
 *
 * @author Jay
 * @date 2022/03/23 15:40
 */
public abstract class TrackerProcessor extends AbstractProcessor {
    /**
     * 命令码 与 请求类的映射
     * 该Map主要用来判断哪些请求需要检查权限
     * 同时通过该Map可以获得请求体的类，因此可以反序列化
     */
    private static final Map<CommandCode, Class<? extends BucketAccessRequest>> REQUEST_CLASS_MAPPING = new HashMap<>();
    final CommandFactory commandFactory;
    final BucketManager bucketManager;

    static {
        /*
            注册需要检查权限的请求
         */
        REQUEST_CLASS_MAPPING.put(FastOssProtocol.LOCATE_OBJECT, LocateObjectRequest.class);
        REQUEST_CLASS_MAPPING.put(FastOssProtocol.LIST_BUCKET, ListBucketRequest.class);
        REQUEST_CLASS_MAPPING.put(FastOssProtocol.DELETE_OBJECT, DeleteObjectInBucketRequest.class);
        REQUEST_CLASS_MAPPING.put(FastOssProtocol.BUCKET_PUT_OBJECT, BucketPutObjectRequest.class);
        REQUEST_CLASS_MAPPING.put(FastOssProtocol.INIT_MULTIPART_UPLOAD, BucketPutObjectRequest.class);
        REQUEST_CLASS_MAPPING.put(FastOssProtocol.LOOKUP_MULTIPART_UPLOAD, LookupMultipartUploadRequest.class);
        REQUEST_CLASS_MAPPING.put(FastOssProtocol.COMPLETE_MULTIPART_UPLOAD, CompleteMultipartUploadRequest.class);
        REQUEST_CLASS_MAPPING.put(FastOssProtocol.GET_OBJECT_META, LocateObjectRequest.class);
    }

    protected TrackerProcessor(CommandFactory commandFactory, BucketManager bucketManager) {
        this.commandFactory = commandFactory;
        this.bucketManager = bucketManager;
    }

    @Override
    public final void process(ChannelHandlerContext context, Object o) {
        if(o instanceof FastOssCommand){
            RemotingCommand response;
            FastOssCommand command = (FastOssCommand) o;
            if(REQUEST_CLASS_MAPPING.containsKey(command.getCommandCode())){
                CommandCode code = filter(command);
                if(code.equals(FastOssProtocol.SUCCESS)){
                    response = doProcess(command);
                }else{
                    response = commandFactory.createResponse(command.getId(), "", code);
                }
            } else{
                response = doProcess(command);
            }
            sendResponse(context, response);
        }
    }

    /**
     * 请求过滤
     * 过滤掉没有访问权限的请求
     * @param command {@link FastOssCommand}
     * @return {@link CommandCode}
     */
    private CommandCode filter(FastOssCommand command){
        Class<? extends BucketAccessRequest> requestClazz = REQUEST_CLASS_MAPPING.get(command.getCommandCode());
        // 反序列化
        BucketAccessRequest request = SerializeUtil.deserialize(command.getContent(), requestClazz);
        // 验证访问权限
        return BucketAclUtil.checkAuthorization(bucketManager, request.bucket(), request.token(), request.accessMode());
    }

    /**
     * 实际的处理逻辑
     * @param command {@link FastOssCommand}
     * @return {@link RemotingCommand}
     */
    public abstract RemotingCommand doProcess(FastOssCommand command);
}
