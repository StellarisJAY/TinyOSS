package com.jay.oss.tracker.processor;

import com.alibaba.fastjson.JSON;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.bitcask.HintIndex;
import com.jay.oss.common.bitcask.Index;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.edit.EditLog;
import com.jay.oss.common.edit.EditLogManager;
import com.jay.oss.common.edit.EditOperation;
import com.jay.oss.common.entity.request.BucketPutObjectRequest;
import com.jay.oss.common.entity.request.ListBucketRequest;
import com.jay.oss.common.entity.bucket.Bucket;
import com.jay.oss.common.entity.bucket.BucketVO;
import com.jay.oss.common.entity.bucket.GetServiceRequest;
import com.jay.oss.common.entity.bucket.GetServiceResponse;
import com.jay.oss.common.entity.object.ObjectMeta;
import com.jay.oss.common.registry.StorageNodeInfo;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.common.util.UrlUtil;
import com.jay.oss.tracker.meta.BucketManager;
import com.jay.oss.tracker.registry.StorageRegistry;
import com.jay.oss.tracker.track.ObjectTracker;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
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

    private final StorageRegistry storageRegistry;
    private final EditLogManager editLogManager;
    private final ObjectTracker objectTracker;

    public BucketProcessor(BucketManager bucketManager, StorageRegistry storageRegistry, EditLogManager editLogManager,
                           ObjectTracker objectTracker, CommandFactory commandFactory) {
        super(commandFactory, bucketManager);
        this.storageRegistry = storageRegistry;
        this.editLogManager = editLogManager;
        this.objectTracker = objectTracker;
    }

    @Override
    public RemotingCommand doProcess(FastOssCommand command) {
        CommandCode code = command.getCommandCode();
        if(FastOssProtocol.PUT_BUCKET.equals(code)){
            return processPutBucket(command);
        }
        else if(FastOssProtocol.LIST_BUCKET.equals(code)){
            return processListBucket(command);
        }
        else if(FastOssProtocol.BUCKET_PUT_OBJECT.equals(code)){
            return bucketPutObject(command);
        }
        else if(FastOssProtocol.GET_SERVICE.equals(code)){
            return processGetService(command);
        }
        return null;
    }

    /**
     * 处理put bucket请求
     * @param command {@link FastOssCommand}
     * @return {@link RemotingCommand}
     */
    private RemotingCommand processPutBucket(FastOssCommand command){
        byte[] content = command.getContent();
        Bucket bucket = SerializeUtil.deserialize(content, Bucket.class);
        // 保存存储桶，并生成appId、AK、SK
        bucket = bucketManager.addBucket(bucket);
        // 记录添加存储桶日志
        appendAddBucketLog(bucket);
        String keyPair = bucket.getAppId() + ";" + bucket.getAccessKey() + ";" + bucket.getSecretKey();
        return commandFactory.createResponse(command.getId(), keyPair, FastOssProtocol.SUCCESS);
    }

    /**
     * 处理list bucket请求
     * @param command {@link FastOssCommand}
     * @return {@link RemotingCommand}
     */
    private RemotingCommand processListBucket(FastOssCommand command){
        byte[] content = command.getContent();
        // 反序列化请求
        ListBucketRequest request = SerializeUtil.deserialize(content, ListBucketRequest.class);
        // list bucket
        List<String> objects = bucketManager.listBucket(request.getBucket(), request.getCount(), request.getOffset());
        // 转换成JSON
        String json = JSON.toJSONString(objects);
        return commandFactory.createResponse(command.getId(), json, FastOssProtocol.SUCCESS);
    }

    /**
     * 处理向桶中放入object元数据
     * @param command {@link FastOssCommand}
     * @return {@link RemotingCommand}
     */
    private RemotingCommand bucketPutObject(FastOssCommand command){
        BucketPutObjectRequest request = SerializeUtil.deserialize(command.getContent(), BucketPutObjectRequest.class);
        String bucket = request.getBucket();
        String objectKey = request.getKey();
        long size = request.getSize();
        RemotingCommand response;
        // 判断桶是否开启了版本控制
        String versionId = "";
        if(bucketManager.getBucket(bucket).isVersioning()){
            // 生成版本号
            versionId = UUID.randomUUID().toString();
            objectKey = objectKey + "/" + versionId;
        }
        try{
            // 选择上传点
            List<StorageNodeInfo> nodes = storageRegistry.selectUploadNode(objectKey, size, OssConfigs.replicaCount());
            String urls = UrlUtil.stringifyFromNodes(nodes);
            ObjectMeta meta = ObjectMeta.builder()
                    .locations(urls).fileName(request.getFilename())
                    .md5(request.getMd5()).objectKey(request.getKey())
                    .size(size).createTime(request.getCreateTime())
                    .versionId(versionId)
                    .build();
            // 保存object位置，判断object是否已经存在
            if(objectTracker.putObjectMeta(objectKey, meta)){
                bucketManager.putObject(bucket, objectKey);
                // 日志记录put object
                appendBucketPutObjectLog(objectKey);
                urls = urls + versionId;
                response = commandFactory.createResponse(command.getId(), urls, FastOssProtocol.SUCCESS);
            }else{
                // object key 重复
                response =commandFactory.createResponse(command.getId(), "", FastOssProtocol.DUPLICATE_OBJECT_KEY);
            }
        }catch (Exception e){
            log.error("bucket put object error ", e);
            response = commandFactory
                    .createResponse(command.getId(), e.getMessage(), FastOssProtocol.NO_ENOUGH_STORAGES);
        }
        return response;
    }

    /**
     * 追加添加Bucket日志
     * @param bucket {@link Bucket}
     */
    private void appendAddBucketLog(Bucket bucket){
        String key = bucket.getBucketName() + "-" + bucket.getAppId();
        Index index = bucketManager.getIndex(key);
        HintIndex hint = new HintIndex(key, index.getChunkId(), index.getOffset(), index.isRemoved());
        byte[] content = SerializeUtil.serialize(hint, HintIndex.class);
        EditLog editLog = new EditLog(EditOperation.ADD, content);
        editLogManager.append(editLog);
    }


    /**
     * 追加Bucket添加Object日志
     * @param objectKey objectKey
     */
    private void appendBucketPutObjectLog(String objectKey){
        Index index = objectTracker.getIndex(objectKey);
        HintIndex hint = new HintIndex(objectKey, index.getChunkId(), index.getOffset(), index.isRemoved());
        byte[] serialized = SerializeUtil.serialize(hint, HintIndex.class);
        EditLog editLog = new EditLog(EditOperation.BUCKET_PUT_OBJECT, serialized);
        editLogManager.append(editLog);
    }


    /**
     * 处理GetService请求
     * 列出系统中的所有存储桶信息
     * @param command {@link FastOssCommand}
     * @return {@link RemotingCommand}
     */
    private RemotingCommand processGetService(FastOssCommand command){
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
        responseCommand = commandFactory.createResponse(command.getId(), serialize, FastOssProtocol.SUCCESS);
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
}
