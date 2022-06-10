package com.jay.oss.tracker.processor;

import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.entity.object.ObjectMeta;
import com.jay.oss.common.entity.request.BucketPutObjectRequest;
import com.jay.oss.common.entity.response.PutObjectMetaResponse;
import com.jay.oss.common.registry.StorageNodeInfo;
import com.jay.oss.common.remoting.TinyOssCommand;
import com.jay.oss.common.remoting.TinyOssProtocol;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.common.util.SnowflakeIdGenerator;
import com.jay.oss.tracker.meta.BucketManager;
import com.jay.oss.tracker.registry.StorageNodeRegistry;
import com.jay.oss.tracker.track.ObjectTracker;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * <p>
 *  Put对象元数据处理器
 * </p>
 *
 * @author Jay
 * @date 2022/04/20 16:25
 */
@Slf4j
public class PutObjectMetaProcessor extends TrackerProcessor{
    private final StorageNodeRegistry storageRegistry;
    private final ObjectTracker objectTracker;
    private final SnowflakeIdGenerator objectIdGenerator;

    public PutObjectMetaProcessor(CommandFactory commandFactory, BucketManager bucketManager,
                                  StorageNodeRegistry storageRegistry,
                                     ObjectTracker objectTracker) {
        super(commandFactory, bucketManager);
        this.storageRegistry = storageRegistry;
        this.objectTracker = objectTracker;
        this.objectIdGenerator = new SnowflakeIdGenerator(0L, 0L);
    }

    @Override
    public RemotingCommand doProcess(TinyOssCommand command) {
        BucketPutObjectRequest request = SerializeUtil.deserialize(command.getContent(), BucketPutObjectRequest.class);
        String bucket = request.getBucket();
        String objectKey = request.getKey();
        int size = request.getSize();
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
            List<StorageNodeInfo> nodes = storageRegistry.selectUploadNode(size, OssConfigs.replicaCount());
            List<String> urls = nodes.stream().map(StorageNodeInfo::getUrl).collect(Collectors.toList());
            // 创建元数据
            ObjectMeta meta = ObjectMeta.builder()
                    .objectId(objectIdGenerator.nextId()).fileName(request.getFilename())
                    .md5(request.getMd5())
                    .size(size).createTime(request.getCreateTime())
                    .versionId(versionId)
                    .build();
            // 保存元数据
            if(objectTracker.putMeta(objectKey, meta)){
                PutObjectMetaResponse putResp = new PutObjectMetaResponse(meta.getObjectId(), urls, versionId);
                response = commandFactory.createResponse(command.getId(), putResp, PutObjectMetaResponse.class, TinyOssProtocol.SUCCESS);
            }else{
                // object key 重复
                response =commandFactory.createResponse(command.getId(), "", TinyOssProtocol.DUPLICATE_OBJECT_KEY);
            }
        }catch (Exception e){
            log.warn("No enough storage node for: {} ", objectKey,  e);
            response = commandFactory
                    .createResponse(command.getId(), e.getMessage(), TinyOssProtocol.NO_ENOUGH_STORAGES);
        }
        return response;
    }
}
