package com.jay.oss.proxy.service;

import com.jay.dove.DoveClient;
import com.jay.dove.transport.Url;
import com.jay.dove.transport.callback.InvokeCallback;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.entity.AsyncBackupRequest;
import com.jay.oss.common.entity.BucketPutObjectRequest;
import com.jay.oss.common.entity.FilePart;
import com.jay.oss.common.entity.UploadRequest;
import com.jay.oss.common.fs.FilePartWrapper;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.common.util.StringUtil;
import com.jay.oss.common.util.UrlUtil;
import com.jay.oss.proxy.entity.Result;
import com.jay.oss.proxy.util.HttpUtil;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpResponse;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
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
    public FullHttpResponse putObject(String key, String bucket, String token, ByteBuf content) throws Exception {
        FullHttpResponse httpResponse;
        long size = content.readableBytes();
        String objectKey = bucket + "/" + key;
        // 向存储桶put object
        FastOssCommand bucketResponse = bucketPutObject(bucket, objectKey, key, size, System.currentTimeMillis(), token);
        CommandCode code = bucketResponse.getCommandCode();
        // 向桶内添加对象记录
        if(code.equals(FastOssProtocol.SUCCESS)){
            // 计算分片个数
            int parts = (int)(size / FilePart.DEFAULT_PART_SIZE + (size % FilePart.DEFAULT_PART_SIZE == 0 ? 0 : 1));
            String str = StringUtil.toString(bucketResponse.getContent());
            int idx = str.lastIndexOf(";");
            String versionId = str.substring(idx + 1);
            // 获取上传点
            List<Url> urls = UrlUtil.parseUrls(str.substring(0, idx));
            if(!StringUtil.isNullOrEmpty(versionId)){
                objectKey = objectKey + "-" + versionId;
            }
            httpResponse = doUpload(urls, content, (int)size, parts, objectKey, key, versionId);
        }else if(code.equals(FastOssProtocol.NO_ENOUGH_STORAGES)){
            httpResponse = HttpUtil.internalErrorResponse("No enough Storages for replica");
        }else{
            httpResponse = HttpUtil.bucketAclResponse(code);
        }
        return httpResponse;
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
        int writeCount = urls.size() / 2 + 1;
        // 没有成功写入的url集合
        List<Url> asyncWriteUrls = new ArrayList<>();
        List<Url> successUrls = new ArrayList<>();
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
                        successUrls.add(url);
                    }else{
                        asyncWriteUrls.add(url);
                    }
                }else{
                    asyncWriteUrls.add(url);
                }
            }catch (Exception e){
                log.warn("upload to storage failed, ", e);
                asyncWriteUrls.add(url);
            }
        }
        if(successReplica == 0){
           httpResponse = HttpUtil.internalErrorResponse("Upload Object Data Failed");
        }else{
            asyncWriteUrls.addAll(urls.subList(i, urls.size()));
            // 提交异步备份
            submitAsyncBackup(objectKey, asyncWriteUrls, successUrls);

            Result result = new Result().message("Success").putData("versionId",versionId);
            httpResponse = HttpUtil.okResponse(result);
        }
        return httpResponse;
    }


    /**
     * 提交异步备份
     * 由备份成功的节点向剩余节点异步地传输副本来完成备份
     * @param objectKey key
     * @param asyncWriteUrls 异步备份节点
     * @param successUrls 上传成功节点
     */
    private void submitAsyncBackup(String objectKey, List<Url> asyncWriteUrls, List<Url> successUrls){
        int successCount = successUrls.size();
        int j = 0;
        int asyncBackupCount = asyncWriteUrls.size();
        for(int i = 0; i < successCount; i ++){
            List<String> urls;
            if(i != successCount - 1 && j < asyncBackupCount){
                // 前 n - 1 个 成功节点，每个负责一个副本的同步
                urls = Collections.singletonList(asyncWriteUrls.get(j++).getOriginalUrl());
            }else if(j < asyncBackupCount){
                // 最后一个成功节点，负责剩下所有副本的同步
                urls = asyncWriteUrls.subList(j, asyncBackupCount)
                        .stream().map(Url::getOriginalUrl)
                        .collect(Collectors.toList());
            }else{
                break;
            }
            // 发送异步备份请求
            AsyncBackupRequest request = new AsyncBackupRequest(objectKey, urls);
            byte[] content = SerializeUtil.serialize(request, AsyncBackupRequest.class);
            FastOssCommand command = (FastOssCommand) storageClient.getCommandFactory()
                    .createRequest(content, FastOssProtocol.ASYNC_BACKUP);
            storageClient.sendOneway(successUrls.get(i), command);
        }
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
    private FastOssCommand bucketPutObject(String bucket, String objectKey, String filename, long size, long createTime, String token)throws Exception{
        // 获取tracker服务器地址
        String tracker = OssConfigs.trackerServerHost();
        Url url = Url.parseString(tracker);
        // 创建bucket put object请求
        BucketPutObjectRequest request = BucketPutObjectRequest.builder()
                .filename(filename).key(objectKey)
                .bucket(bucket).size(size).token(token)
                .createTime(createTime).build();
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


    /**
     * 分片上传回调
     */
    static class UploadCallback implements InvokeCallback{
        private final CompletableFuture<FastOssCommand> responseFuture;
        private final int partNum;
        private final String key;
        public UploadCallback(CompletableFuture<FastOssCommand> responseFuture, int partNum, String key) {
            this.responseFuture = responseFuture;
            this.partNum = partNum;
            this.key = key;
        }

        @Override
        public void onComplete(RemotingCommand remotingCommand) {
            if(remotingCommand instanceof FastOssCommand){
                FastOssCommand response = (FastOssCommand) remotingCommand;
                CommandCode code = response.getCommandCode();
                if(code.equals(FastOssProtocol.ERROR)){
                    // 上传分片出错, 重传
                    responseFuture.completeExceptionally(new RuntimeException("Upload Parts Failed"));
                }else if(code.equals(FastOssProtocol.RESPONSE_UPLOAD_DONE)){
                    // 收到所有分片，上传成功
                    responseFuture.complete(response);
                }
            }
        }

        @Override
        public void exceptionCaught(Throwable throwable) {
            responseFuture.completeExceptionally(throwable);
        }

        @Override
        public void onTimeout(RemotingCommand remotingCommand) {
            log.warn("upload file part timeout, part number: {}, key: {}", partNum, key);
        }

        @Override
        public ExecutorService getExecutor() {
            return null;
        }
    }
}
