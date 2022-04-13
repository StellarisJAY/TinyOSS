package com.jay.oss.proxy.service;

import com.jay.dove.DoveClient;
import com.jay.dove.transport.Url;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.acl.BucketAccessMode;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.entity.request.AsyncBackupRequest;
import com.jay.oss.common.entity.request.BucketPutObjectRequest;
import com.jay.oss.common.entity.FilePart;
import com.jay.oss.common.entity.request.UploadRequest;
import com.jay.oss.common.fs.FilePartWrapper;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.util.KeyUtil;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.common.util.StringUtil;
import com.jay.oss.common.util.UrlUtil;
import com.jay.oss.proxy.callback.UploadCallback;
import com.jay.oss.proxy.entity.Result;
import com.jay.oss.proxy.util.HttpUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.FullHttpResponse;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

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
        FullHttpResponse httpResponse;
        long size = content.readableBytes();
        String objectKey = KeyUtil.getObjectKey(key, bucket, null);
        // 向存储桶put object
        FastOssCommand bucketResponse = bucketPutObject(bucket, objectKey, key, size, System.currentTimeMillis(), token, md5);
        CommandCode code = bucketResponse.getCommandCode();
        // 向桶内添加对象记录
        if(code.equals(FastOssProtocol.SUCCESS)){
            String str = StringUtil.toString(bucketResponse.getContent());
            String[] parts = str.split(";");
            int replicaCount = OssConfigs.replicaCount();
            if(parts.length >= replicaCount + 1){
                long objectId = Long.parseLong(parts[replicaCount]);
                List<Url> storages = UrlUtil.parseUrls(parts, replicaCount);
                if(doUpload(storages, content, (int)size, objectId)){
                    Result result = new Result().message("Success")
                            .putData("versionId", parts.length > replicaCount + 1 ? parts[replicaCount + 1] : null);
                    httpResponse = HttpUtil.okResponse(result);
                }else{
                    httpResponse = HttpUtil.internalErrorResponse("Failed to Upload Object");
                }
            }else{
                httpResponse = HttpUtil.internalErrorResponse("Failed to Upload Object");
            }
        }else if(code.equals(FastOssProtocol.NO_ENOUGH_STORAGES)){
            httpResponse = HttpUtil.internalErrorResponse("No enough Storages for replica");
        }else{
            httpResponse = HttpUtil.bucketAclResponse(code);
        }
        return httpResponse;
    }

    private boolean doUpload(List<Url> urls, ByteBuf content, int size, long objectId){
        int successReplica = 0;
        // 需要写入成功的数量
        int writeCount = 1;
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeLong(objectId);
        buffer.writeLong(size);
        buffer.writeBytes(content);
        int i;
        for(i = 0; i < urls.size() && successReplica < writeCount; i++){
            Url url = urls.get(i);
            try{
                buffer.markReaderIndex();
                buffer.retain();
                RemotingCommand command = storageClient.getCommandFactory()
                        .createRequest(buffer, FastOssProtocol.UPLOAD_REQUEST);
                RemotingCommand response = storageClient.sendSync(url, command, null);
                buffer.resetReaderIndex();
                buffer.release();
                if(response.getCommandCode().equals(FastOssProtocol.SUCCESS)){
                    successReplica ++;
                }
            }catch (Exception e){
                log.warn("upload to storage failed, ", e);
            }
        }
        return successReplica != 0;
    }

    /**
     * 完成上传数据
     * @param urls 目标Storage节点集合
     * @param content 完整数据
     * @param size 数据大小
     * @param parts 分片个数
     * @param objectKey 对象Key
     */
    private FullHttpResponse doUpload(List<Url> urls, ByteBuf content, int size, int parts, String objectKey, String fileName, String versionId){
        FullHttpResponse httpResponse;
        int successReplica = 0;
        // 需要写入成功的数量
        int writeCount = 1;
        UploadRequest request = UploadRequest.builder().size(size).key(objectKey).filename(fileName).parts(parts).build();
        int i;
        for(i = 0; i < urls.size() && successReplica < writeCount; i++){
            Url url = urls.get(i);
            try{
                // 上传元数据
                FastOssCommand uploadMeta = uploadFileHeader(url, request);
                if(uploadMeta.getCommandCode().equals(FastOssProtocol.SUCCESS)){
                    // 上传分片
                    FastOssCommand response = uploadFileParts(url, content, size, parts, objectKey);
                    if(response.getCommandCode().equals(FastOssProtocol.RESPONSE_UPLOAD_DONE)){
                        // 全部分片上传成功
                        successReplica++;
                    }
                }
            }catch (Exception e){
                log.warn("upload to storage failed, ", e);
            }
        }
        if(successReplica == 0){
           httpResponse = HttpUtil.internalErrorResponse("Upload Object Data Failed");
        }else{
            Result result = new Result().message("Success").putData("versionId",versionId);
            httpResponse = HttpUtil.okResponse(result);
        }
        return httpResponse;
    }

    /**
     * 存储桶put object
     * 同时校验访问权限 和 分配上传点
     * @param bucket bucket
     * @param filename 对象名称
     * @param objectKey Object key
     * @param size 大小
     * @param createTime 创建时间
     * @param token 访问token
     * @return {@link FastOssCommand}
     * @throws Exception e
     */
    private FastOssCommand bucketPutObject(String bucket, String objectKey, String filename, long size, long createTime, String token, String md5)throws Exception{
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
                .createRequest(request, FastOssProtocol.BUCKET_PUT_OBJECT, BucketPutObjectRequest.class);
        return (FastOssCommand) storageClient.sendSync(url, command, null);
    }

    /**
     * 上传文件header
     * 该请求会在storage开启一个FileReceiver准备接收数据
     * @param url 目标url {@link Url}
     * @param request {@link UploadRequest}
     * @return {@link FastOssCommand}
     * @throws InterruptedException e
     */
    private FastOssCommand uploadFileHeader(Url url, UploadRequest request) throws InterruptedException {
        // 创建请求报文
        RemotingCommand command = storageClient.getCommandFactory()
                .createRequest(request, FastOssProtocol.UPLOAD_FILE_HEADER, UploadRequest.class);
        return (FastOssCommand) storageClient.sendSync(url, command, null);
    }

    /**
     * 分片上传文件到存储节点
     * @param url {@link Url} 存储节点url
     * @param content {@link ByteBuf} 数据
     * @param size long 数据大小
     * @param parts 分片个数
     * @param key 文件key
     * @return {@link FastOssCommand} upload parts response
     */
    private FastOssCommand uploadFileParts(Url url, ByteBuf content, long size, int parts, String key) throws Exception {
        CommandFactory commandFactory = storageClient.getCommandFactory();
        byte[] keyBytes = StringUtil.getBytes(key);
        int keyLength = keyBytes.length;
        try{
            // future, 等待所有分片上传完成
            CompletableFuture<FastOssCommand> responseFuture = new CompletableFuture<>();
            for(int i = 0; i < parts; i++){
                // 计算当前分片大小
                int partSize = i == parts - 1 ? (int) size % FilePart.DEFAULT_PART_SIZE : FilePart.DEFAULT_PART_SIZE;
                // 封装part
                FilePartWrapper partWrapper = FilePartWrapper.builder()
                        .key(keyBytes)
                        .fullContent(content)
                        .length(partSize)
                        .keyLength(keyLength)
                        .index(i * FilePart.DEFAULT_PART_SIZE)
                        .partNum(i).build();
                // 将content refCnt + 1
                content.retain();
                // 创建请求
                RemotingCommand request = commandFactory.createRequest(partWrapper, FastOssProtocol.UPLOAD_FILE_PARTS);
                // 发送文件分片，异步方式发送
                storageClient.sendAsync(url, request, new UploadCallback(responseFuture, i, key));
            }
            return responseFuture.get();
        }catch (Exception e){
            log.warn("upload file parts failed, cause: ", e);
            throw e;
        }
    }

}
