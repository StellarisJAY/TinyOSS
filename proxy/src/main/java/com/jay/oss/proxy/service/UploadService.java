package com.jay.oss.proxy.service;

import com.jay.dove.DoveClient;
import com.jay.dove.transport.Url;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.acl.BucketAccessMode;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.entity.request.BucketPutObjectRequest;
import com.jay.oss.common.entity.response.PutObjectMetaResponse;
import com.jay.oss.common.remoting.TinyOssCommand;
import com.jay.oss.common.remoting.TinyOssProtocol;
import com.jay.oss.common.util.KeyUtil;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.common.util.StringUtil;
import com.jay.oss.proxy.entity.Result;
import com.jay.oss.proxy.util.HttpUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.FullHttpResponse;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.StringJoiner;

/**
 * <p>
 *  Proxy端处理上传请求
 * </p>
 *
 * @author Jay
 * @date 2022/01/25 10:39
 */
@Slf4j
public class UploadService {
    /**
     * 存储节点客户端，其中管理了多个存储节点的连接
     */
    private final DoveClient storageClient;

    public UploadService(DoveClient storageClient) {
        this.storageClient = storageClient;
    }


    /**
     * put object
     * @param key object key
     * @param bucket 桶
     * @param token 访问token
     * @param content object数据
     * @return {@link FullHttpResponse}
     * @throws Exception e
     */
    public FullHttpResponse putObject(String key, String bucket, String token, String md5, ByteBuf content) throws Exception {
        if(StringUtil.isNullOrEmpty(key) || StringUtil.isNullOrEmpty(bucket) || content.readableBytes() == 0){
            return HttpUtil.badRequestResponse("Missing important parameters for Put Object");
        }
        long size = content.readableBytes();
        String objectKey = KeyUtil.getObjectKey(key, bucket, null);
        // 向存储桶put object
        TinyOssCommand bucketResponse = bucketPutObject(bucket, objectKey, key, size, System.currentTimeMillis(), token, md5);
        CommandCode code = bucketResponse.getCommandCode();
        // 向桶内添加对象记录
        if(!code.equals(TinyOssProtocol.SUCCESS)){
            return HttpUtil.errorResponse(code);
        }else {
            PutObjectMetaResponse resp = SerializeUtil.deserialize(bucketResponse.getContent(), PutObjectMetaResponse.class);
            if(doUpload(resp.getLocations(), content, (int)size, resp.getObjectId())){
                Result result = new Result().message("Success")
                        .putData("versionId", resp.getVersionId());
                return HttpUtil.okResponse(result);
            }else{
                return HttpUtil.internalErrorResponse("Failed to Upload Object");
            }
        }
    }

    /**
     * 向Storage服务器发送对象
     * 会根据副本策略向一定数量的storage发送数据
     * 如果成功收到对象的服务器数量满足策略要求，则返回成功。
     * @param urls storage服务器列表
     * @param content 对象数据
     * @param size 对象大小
     * @param objectId 对象ID
     * @return 发送状态
     */
    private boolean doUpload(List<String> urls, ByteBuf content, int size, long objectId){
        int successReplica = 0;
        // 需要写入成功的数量
        int writeCount = 1;
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeLong(objectId);
        buffer.writeLong(size);
        buffer.writeBytes(content);
        Queue<String> urlQueue = new LinkedList<>(urls);
        int urlCount = urlQueue.size();
        for(int i = 0; i < urlCount && successReplica < writeCount; i++){
            String urlStr = urlQueue.poll();
            if(urlStr == null){
                break;
            }
            Url url = Url.parseString(urlStr);
            try{
                buffer.markWriterIndex();
                StringJoiner joiner = new StringJoiner(";");
                urlQueue.forEach(joiner::add);
                buffer.writeBytes(StringUtil.getBytes(joiner.toString()));
                buffer.markReaderIndex();
                buffer.retain();
                RemotingCommand command = storageClient.getCommandFactory()
                        .createRequest(buffer, TinyOssProtocol.UPLOAD_REQUEST);
                RemotingCommand response = storageClient.sendSync(url, command, null);
                buffer.resetReaderIndex();
                buffer.resetWriterIndex();
                buffer.release();
                if(response.getCommandCode().equals(TinyOssProtocol.SUCCESS)){
                    successReplica ++;
                }else{
                    urlQueue.offer(urlStr);
                }
            }catch (Exception e){
                log.warn("upload to storage failed, ", e);
            }
        }
        return successReplica != 0;
    }

    /**
     * 存储桶put object
     * 向Tracker发送对象元数据
     * 同时校验访问权限 和 分配上传点
     * @param bucket bucket
     * @param filename 对象名称
     * @param objectKey Object key
     * @param size 大小
     * @param createTime 创建时间
     * @param token 访问token
     * @return {@link TinyOssCommand}
     * @throws Exception e
     */
    private TinyOssCommand bucketPutObject(String bucket, String objectKey, String filename, long size, long createTime, String token, String md5)throws Exception{
        // 获取tracker服务器地址
        String tracker = OssConfigs.trackerServerHost();
        Url url = Url.parseString(tracker);
        // 创建bucket put object请求
        BucketPutObjectRequest request = BucketPutObjectRequest.builder()
                .filename(filename).key(objectKey).bucket(bucket)
                .size(size).token(token)
                .createTime(createTime)
                .md5(md5 == null ? "" : md5)
                .accessMode(BucketAccessMode.WRITE)
                .build();
        // 发送
        RemotingCommand command = storageClient.getCommandFactory()
                .createRequest(request, TinyOssProtocol.BUCKET_PUT_OBJECT, BucketPutObjectRequest.class);
        return (TinyOssCommand) storageClient.sendSync(url, command, null);
    }

}
