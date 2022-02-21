package com.jay.oss.tracker.processor;

import com.alibaba.fastjson.JSON;
import com.jay.dove.transport.command.AbstractProcessor;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.oss.common.acl.BucketAccessMode;
import com.jay.oss.common.entity.SelectUploadNodeRequest;
import com.jay.oss.common.registry.StorageNodeInfo;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.tracker.meta.BucketManager;
import com.jay.oss.tracker.meta.ObjectTracker;
import com.jay.oss.tracker.registry.StorageRegistry;
import com.jay.oss.tracker.util.BucketAclUtil;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * <p>
 *  object相关处理器
 *  处理object定位、object上传点选择
 * </p>
 *
 * @author Jay
 * @date 2022/02/21 11:06
 */
@Slf4j
public class ObjectProcessor extends AbstractProcessor {

    private final ObjectTracker objectTracker;
    private final StorageRegistry storageRegistry;
    private final CommandFactory commandFactory;
    private final BucketManager bucketManager;

    public ObjectProcessor(ObjectTracker objectTracker, BucketManager bucketManager, StorageRegistry storageRegistry, CommandFactory commandFactory) {
        this.objectTracker = objectTracker;
        this.commandFactory = commandFactory;
        this.bucketManager = bucketManager;
        this.storageRegistry = storageRegistry;
    }

    @Override
    public void process(ChannelHandlerContext channelHandlerContext, Object o) {
        if(o instanceof FastOssCommand){
            FastOssCommand command = (FastOssCommand) o;
            CommandCode code = command.getCommandCode();

            if(FastOssProtocol.SELECT_UPLOAD_NODE.equals(code)){
                selectUploadNode(channelHandlerContext, command);
            }
            else if(FastOssProtocol.LOCATE_OBJECT.equals(code)){
                locateObject(channelHandlerContext, command);
            }
        }
    }

    private void selectUploadNode(ChannelHandlerContext context, FastOssCommand command){
        byte[] content = command.getContent();
        SelectUploadNodeRequest request = SerializeUtil.deserialize(content, SelectUploadNodeRequest.class);
        // 检查存储桶访问权限
        CommandCode code = BucketAclUtil
                .checkAuthorization(bucketManager, request.getBucket(), request.getToken(), BucketAccessMode.WRITE);
        FastOssCommand response;
        // 拒绝访问
        if(!code.equals(FastOssProtocol.SUCCESS)){
            response = (FastOssCommand)commandFactory.createResponse(command.getId(), "", code);
        }
        else{
            List<StorageNodeInfo> nodes = storageRegistry.selectUploadNode(request.getSize(), 1);
            StringBuilder builder = new StringBuilder();
            for (StorageNodeInfo node : nodes) {
                builder.append(node.getUrl());
                builder.append(";");
            }
            response = (FastOssCommand) commandFactory.createResponse(command.getId(), builder.toString(), code);
        }
        sendResponse(context, response);
    }

    private void locateObject(ChannelHandlerContext context, FastOssCommand command){

    }
}
