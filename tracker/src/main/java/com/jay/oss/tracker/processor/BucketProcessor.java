package com.jay.oss.tracker.processor;

import com.alibaba.fastjson.JSON;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.entity.bucket.*;
import com.jay.oss.common.entity.request.ListBucketRequest;
import com.jay.oss.common.remoting.TinyOssCommand;
import com.jay.oss.common.remoting.TinyOssProtocol;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.tracker.meta.BucketManager;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * <p>
 *  存储桶请求处理器
 * </p>
 *
 * @author Jay
 * @date 2022/02/21 10:12
 */
@Slf4j
public class BucketProcessor extends TrackerProcessor {

    public BucketProcessor(BucketManager bucketManager, CommandFactory commandFactory) {
        super(commandFactory, bucketManager);
    }

    @Override
    public RemotingCommand doProcess(TinyOssCommand command) {
        CommandCode code = command.getCommandCode();
        if(TinyOssProtocol.PUT_BUCKET.equals(code)){
            return processPutBucket(command);
        }
        else if(TinyOssProtocol.LIST_BUCKET.equals(code)){
            return processListBucket(command);
        }
        else if(TinyOssProtocol.GET_SERVICE.equals(code)){
            return processGetService(command);
        }
        else if(TinyOssProtocol.UPDATE_BUCKET_ACL.equals(code)){
            return processUpdateAcl(command);
        }
        return null;
    }

    /**
     * 处理put bucket请求
     * @param command {@link TinyOssCommand}
     * @return {@link RemotingCommand}
     */
    private RemotingCommand processPutBucket(TinyOssCommand command){
        byte[] content = command.getContent();
        Bucket bucket = SerializeUtil.deserialize(content, Bucket.class);
        // 保存存储桶，并生成appId、AK、SK
        bucket = bucketManager.addBucket(bucket);
        String keyPair = bucket.getAppId() + ";" + bucket.getAccessKey() + ";" + bucket.getSecretKey();
        return commandFactory.createResponse(command.getId(), keyPair, TinyOssProtocol.SUCCESS);
    }

    /**
     * 处理list bucket请求
     * @param command {@link TinyOssCommand}
     * @return {@link RemotingCommand}
     */
    private RemotingCommand processListBucket(TinyOssCommand command){
        byte[] content = command.getContent();
        // 反序列化请求
        ListBucketRequest request = SerializeUtil.deserialize(content, ListBucketRequest.class);
        // list bucket
        List<String> objects = bucketManager.listBucket(request.getBucket(), request.getCount(), request.getOffset());
        // 转换成JSON
        String json = JSON.toJSONString(objects);
        return commandFactory.createResponse(command.getId(), json, TinyOssProtocol.SUCCESS);
    }


    /**
     * 处理GetService请求
     * 列出系统中的所有存储桶信息
     * @param command {@link TinyOssCommand}
     * @return {@link RemotingCommand}
     */
    private RemotingCommand processGetService(TinyOssCommand command){
        GetServiceRequest request = SerializeUtil.deserialize(command.getContent(), GetServiceRequest.class);
        int count = request.getCount();
        int offset = request.getOffset();
        List<String> buckets = bucketManager.listBuckets();
        RemotingCommand responseCommand;
        GetServiceResponse response;
        if(offset >= buckets.size()){
            response = new GetServiceResponse(new ArrayList<>(), buckets.size());
        }
        else{
            buckets.sort(String::compareTo);
            List<BucketVO> vos = buckets.subList(offset, Math.min(offset + count, buckets.size()))
                    .stream().map(key -> {
                        Bucket bucket = bucketManager.getBucket(key);
                        return bucketToVO(bucket);
                    }).collect(Collectors.toList());
            response = new GetServiceResponse(vos, buckets.size());
        }
        byte[] serialize = SerializeUtil.serialize(response, GetServiceResponse.class);
        responseCommand = commandFactory.createResponse(command.getId(), serialize, TinyOssProtocol.SUCCESS);
        return responseCommand;
    }


    /**
     * Bucket元数据转换成VO
     * @param bucket {@link Bucket}
     * @return {@link BucketVO}
     */
    private BucketVO bucketToVO(Bucket bucket){
        return BucketVO.builder()
                .acl(bucket.getAcl()).bucketName(bucket.getBucketName())
                .appId(bucket.getAppId()).versioning(bucket.isVersioning())
                .build();
    }

    /**
     * 处理修改存储桶acl的请求
     * @param command {@link TinyOssCommand}
     * @return {@link RemotingCommand}
     */
    private RemotingCommand processUpdateAcl(TinyOssCommand command){
        UpdateAclRequest request = SerializeUtil.deserialize(command.getContent(), UpdateAclRequest.class);
        String bucketKey = request.getBucket();
        Bucket bucket = bucketManager.getBucket(bucketKey);
        if(bucket == null){
            return commandFactory.createResponse(command.getId(), "", TinyOssProtocol.NOT_FOUND);
        }
        bucket.setAcl(request.getAcl());
        if(bucketManager.updateBucket(bucketKey, bucket)){
            return commandFactory.createResponse(command.getId(), "", TinyOssProtocol.SUCCESS);
        }else{
            return commandFactory.createResponse(command.getId(), "", TinyOssProtocol.ERROR);
        }
    }
}
