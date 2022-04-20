package com.jay.oss.tracker.processor;

import com.jay.dove.transport.command.AbstractProcessor;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.entity.bucket.UpdateAclRequest;
import com.jay.oss.common.entity.request.*;
import com.jay.oss.common.remoting.TinyOssCommand;
import com.jay.oss.common.remoting.TinyOssProtocol;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.common.util.StringUtil;
import com.jay.oss.tracker.meta.BucketManager;
import com.jay.oss.tracker.util.BucketAclUtil;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

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
@Slf4j
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
        REQUEST_CLASS_MAPPING.put(TinyOssProtocol.LOCATE_OBJECT, LocateObjectRequest.class);
        REQUEST_CLASS_MAPPING.put(TinyOssProtocol.LIST_BUCKET, ListBucketRequest.class);
        REQUEST_CLASS_MAPPING.put(TinyOssProtocol.DELETE_OBJECT, DeleteObjectInBucketRequest.class);
        REQUEST_CLASS_MAPPING.put(TinyOssProtocol.BUCKET_PUT_OBJECT, BucketPutObjectRequest.class);
        REQUEST_CLASS_MAPPING.put(TinyOssProtocol.INIT_MULTIPART_UPLOAD, BucketPutObjectRequest.class);
        REQUEST_CLASS_MAPPING.put(TinyOssProtocol.LOOKUP_MULTIPART_UPLOAD, LookupMultipartUploadRequest.class);
        REQUEST_CLASS_MAPPING.put(TinyOssProtocol.COMPLETE_MULTIPART_UPLOAD, CompleteMultipartUploadRequest.class);
        REQUEST_CLASS_MAPPING.put(TinyOssProtocol.GET_OBJECT_META, LocateObjectRequest.class);
        REQUEST_CLASS_MAPPING.put(TinyOssProtocol.UPDATE_BUCKET_ACL, UpdateAclRequest.class);
    }

    protected TrackerProcessor(CommandFactory commandFactory, BucketManager bucketManager) {
        this.commandFactory = commandFactory;
        this.bucketManager = bucketManager;
    }

    @Override
    public final void process(ChannelHandlerContext context, Object o) {
        if(o instanceof TinyOssCommand){
            TinyOssCommand command = (TinyOssCommand) o;
            try{
                Class<? extends BucketAccessRequest> requestClazz;
                if((requestClazz = REQUEST_CLASS_MAPPING.get(command.getCommandCode())) != null){
                    CommandCode auth = checkAuthorization(command, requestClazz);
                    if(auth.equals(TinyOssProtocol.SUCCESS)){
                        sendResponse(context, doProcess(command));
                    }else{
                        sendResponse(context, commandFactory.createResponse(command.getId(), "", auth));
                    }
                }else{
                    sendResponse(context, doProcess(command));
                }
            }catch (Exception e){
                log.error("Tracker Processor Error ", e);
                sendResponse(context, commandFactory.createResponse(command.getId(), e.getMessage(), TinyOssProtocol.ERROR));
            }
        }
    }

    /**
     * 请求过滤
     * 过滤掉没有访问权限的请求
     * @param command {@link TinyOssCommand}
     * @param requestClazz {@link Class}
     * @return {@link CommandCode}
     */
    private CommandCode checkAuthorization(TinyOssCommand command, Class<? extends BucketAccessRequest> requestClazz){
        // 反序列化
        BucketAccessRequest request = SerializeUtil.deserialize(command.getContent(), requestClazz);
        if(StringUtil.isNullOrEmpty(request.bucket())){
            return TinyOssProtocol.ACCESS_DENIED;
        }
        // 验证访问权限
        return BucketAclUtil.checkAuthorization(bucketManager, request.bucket(), request.token(), request.accessMode());
    }

    /**
     * 实际的处理逻辑
     * @param command {@link TinyOssCommand}
     * @return {@link RemotingCommand}
     * @throws Exception exception
     */
    public abstract RemotingCommand doProcess(TinyOssCommand command) throws Exception;
}
