package com.jay.oss.tracker.processor;

import com.alibaba.fastjson.JSON;
import com.jay.dove.transport.command.AbstractProcessor;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.acl.BucketAccessMode;
import com.jay.oss.common.bitcask.HintIndex;
import com.jay.oss.common.bitcask.Index;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.edit.EditLog;
import com.jay.oss.common.edit.EditLogManager;
import com.jay.oss.common.edit.EditOperation;
import com.jay.oss.common.entity.bucket.Bucket;
import com.jay.oss.common.entity.BucketPutObjectRequest;
import com.jay.oss.common.entity.DeleteObjectInBucketRequest;
import com.jay.oss.common.entity.ListBucketRequest;
import com.jay.oss.common.entity.bucket.BucketEntity;
import com.jay.oss.common.entity.object.ObjectMeta;
import com.jay.oss.common.registry.StorageNodeInfo;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.common.util.UrlUtil;
import com.jay.oss.tracker.db.SqlUtil;
import com.jay.oss.tracker.mapper.BucketMapper;
import com.jay.oss.tracker.meta.BucketManager;
import com.jay.oss.tracker.registry.StorageRegistry;
import com.jay.oss.tracker.track.ObjectTracker;
import com.jay.oss.tracker.util.BucketAclUtil;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.SqlSession;

import java.util.List;
import java.util.UUID;

/**
 * <p>
 *  存储桶请求处理器
 * </p>
 *
 * @author Jay
 * @date 2022/02/21 10:12
 */
@Slf4j
public class BucketProcessor extends AbstractProcessor {

    private final BucketManager bucketManager;
    private final StorageRegistry storageRegistry;
    private final CommandFactory commandFactory;
    private final EditLogManager editLogManager;
    private final ObjectTracker objectTracker;
    private final SqlUtil sqlUtil;

    public BucketProcessor(BucketManager bucketManager, StorageRegistry storageRegistry, EditLogManager editLogManager,
                           ObjectTracker objectTracker, SqlUtil sqlUtil, CommandFactory commandFactory) {
        this.bucketManager = bucketManager;
        this.commandFactory = commandFactory;
        this.sqlUtil = sqlUtil;
        this.storageRegistry = storageRegistry;
        this.editLogManager = editLogManager;
        this.objectTracker = objectTracker;
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
        // 保存存储桶，并生成appId、AK、SK
        bucket = bucketManager.addBucket(bucket);
        // 记录添加存储桶日志
        appendAddBucketLog(bucket);
        String keyPair = bucket.getAppId() + ";" + bucket.getAccessKey() + ";" + bucket.getSecretKey();
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
            List<String> objects = bucketManager.listBucket(request.getBucket(), request.getCount(), request.getOffset());
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
        String objectKey = request.getKey();
        long size = request.getSize();
        // 检查存储桶访问权限
        CommandCode code = BucketAclUtil
                .checkAuthorization(bucketManager, bucket, token, BucketAccessMode.WRITE);
        RemotingCommand response;
        // 拥有权限，完成put object
        if(code.equals(FastOssProtocol.SUCCESS)){
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
                    response = commandFactory.createResponse(command.getId(), urls, code);
                }else{
                    // object key 重复
                    response =commandFactory.createResponse(command.getId(), "", FastOssProtocol.DUPLICATE_OBJECT_KEY);
                }
            }catch (Exception e){
                log.error("bucket put object error ", e);
                response = commandFactory
                        .createResponse(command.getId(), e.getMessage(), FastOssProtocol.NO_ENOUGH_STORAGES);
            }
        }else{
            // 没有访问权限 或者 存储桶不存在
            response = commandFactory.createResponse(command.getId(), "", code);
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
            boolean delete = bucketManager.deleteObject(request.getBucket(), request.getObjectKey());
            if(!delete){
                // 删除失败，object不存在
                response = (FastOssCommand) commandFactory
                        .createResponse(command.getId(), "", FastOssProtocol.NOT_FOUND);
            }else{
                appendBucketDeleteObjectLog(request.getObjectKey());
                response = (FastOssCommand) commandFactory
                        .createResponse(command.getId(), "", FastOssProtocol.SUCCESS);
            }
        }else{
            response = (FastOssCommand) commandFactory
                    .createResponse(command.getId(), "", code);
        }
        sendResponse(context, response);
    }


    private void appendAddBucketLog(Bucket bucket){
        String key = bucket.getBucketName() + "-" + bucket.getAppId();
        Index index = bucketManager.getIndex(key);
        HintIndex hint = new HintIndex(key, index.getChunkId(), index.getOffset(), index.isRemoved());
        byte[] content = SerializeUtil.serialize(hint, HintIndex.class);
        EditLog editLog = new EditLog(EditOperation.ADD, content);
        editLogManager.append(editLog);
    }

    private void appendBucketPutObjectLog(String objectKey){
        Index index = objectTracker.getIndex(objectKey);
        HintIndex hint = new HintIndex(objectKey, index.getChunkId(), index.getOffset(), index.isRemoved());
        byte[] serialized = SerializeUtil.serialize(hint, HintIndex.class);
        EditLog editLog = new EditLog(EditOperation.BUCKET_PUT_OBJECT, serialized);
        editLogManager.append(editLog);
    }

    private void appendBucketDeleteObjectLog(String objectKey){
        EditLog editLog = new EditLog(EditOperation.BUCKET_DELETE_OBJECT, objectKey.getBytes(OssConfigs.DEFAULT_CHARSET));
        editLogManager.append(editLog);
    }

    private void saveBucketToMySQL(Bucket bucket){
        if(OssConfigs.enableMysql()){
            SqlSession session = sqlUtil.getSession(true);
            BucketMapper mapper = session.getMapper(BucketMapper.class);
            BucketEntity bucketEntity = BucketEntity.builder()
                    .bucketName(bucket.getBucketName()).appId(bucket.getAppId())
                    .createTime(System.currentTimeMillis()).ownerId(0L)
                    .secretKey(bucket.getSecretKey()).accessKey(bucket.getAccessKey())
                    .versioning(bucket.isVersioning()).acl(bucket.getAcl().code)
                    .build();
            mapper.insertBucket(bucketEntity);
            session.close();
        }
    }
}
