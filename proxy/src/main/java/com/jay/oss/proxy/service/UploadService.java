package com.jay.oss.proxy.service;

import com.jay.dove.DoveClient;
import com.jay.dove.config.DoveConfigs;
import com.jay.dove.transport.Url;
import com.jay.dove.transport.callback.InvokeCallback;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.entity.BucketPutObjectRequest;
import com.jay.oss.common.entity.FilePart;
import com.jay.oss.common.entity.UploadRequest;
import com.jay.oss.common.fs.FilePartWrapper;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.util.HttpUtil;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.common.util.StringUtil;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpResponse;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

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
        // 向存储桶put object
        FastOssCommand bucketResponse = bucketPutObject(bucket, key, size, System.currentTimeMillis(), token);
        // 向桶内添加对象记录
        if(bucketResponse.getCommandCode().equals(FastOssProtocol.SUCCESS)){
            // 计算分片个数
            int parts = (int)(size / FilePart.DEFAULT_PART_SIZE + (size % FilePart.DEFAULT_PART_SIZE == 0 ? 0 : 1));
            // 获取上传点
            List<Url> urls = parseUploadUrls(bucketResponse.getContent());
            Url url = urls.get(0);
            // 创建上传请求
            UploadRequest request = UploadRequest.builder()
                    .key(bucket + key)
                    .filename(key).size(size)
                    .parts(parts).build();
            // 向存储节点发送文件信息，开启上传任务
            FastOssCommand uploadHeaderResponse = uploadFileHeader(url, request);
            CommandCode responseCode = uploadHeaderResponse.getCommandCode();
            // 检查上传头状态
            if(FastOssProtocol.SUCCESS.equals(responseCode)){
                // 上传文件分片
                FastOssCommand response = uploadFileParts(url, content, size, parts, bucket + key);
                httpResponse = HttpUtil.okResponse();
            }else{
                log.warn("upload file header failed, key: {}, bucket: {}", key, bucket);
                httpResponse = HttpUtil.internalErrorResponse("upload file header failed");
            }
        }else if(bucketResponse.getCommandCode().equals(FastOssProtocol.ACCESS_DENIED)){
            // bucket返回拒绝访问
            httpResponse = HttpUtil.forbiddenResponse("Bucket Access Denied");
        }else if(bucketResponse.getCommandCode().equals(FastOssProtocol.NOT_FOUND)){
            // bucket不存在
            httpResponse = HttpUtil.notFoundResponse("Bucket Not Found");
        }else{
            httpResponse = HttpUtil.internalErrorResponse("No enough storages for replica");
        }
        return httpResponse;
    }

    /**
     * 想存储桶put object
     * 同时校验访问权限 和 分配上传点
     * @param bucket bucket
     * @param filename 对象名称
     * @param size 大小
     * @param createTime 创建时间
     * @param token 访问token
     * @return {@link FastOssCommand}
     * @throws Exception e
     */
    private FastOssCommand bucketPutObject(String bucket, String filename, long size, long createTime, String token)throws Exception{
        // 获取tracker服务器地址
        String tracker = OssConfigs.trackerServerHost();
        Url url = Url.parseString(tracker);
        // 创建bucket put object请求
        BucketPutObjectRequest request = BucketPutObjectRequest.builder()
                .filename(filename).key(bucket + filename)
                .bucket(bucket).size(size).token(token)
                .createTime(createTime).build();
        // 序列化
        byte[] content = SerializeUtil.serialize(request, BucketPutObjectRequest.class);
        // 发送
        FastOssCommand command = (FastOssCommand) storageClient.getCommandFactory()
                .createRequest(content, FastOssProtocol.BUCKET_PUT_OBJECT);
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
        // 序列化
        byte[] content = SerializeUtil.serialize(request, UploadRequest.class);
        // 创建请求报文
        RemotingCommand command = storageClient.getCommandFactory()
                .createRequest(content, FastOssProtocol.UPLOAD_FILE_HEADER);
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
        byte[] keyBytes = key.getBytes(DoveConfigs.DEFAULT_CHARSET);
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
                FastOssCommand request = (FastOssCommand)commandFactory.createRequest(partWrapper, FastOssProtocol.UPLOAD_FILE_PARTS);
                // 发送文件分片，异步方式发送
                storageClient.sendAsync(url, request, new UploadCallback(responseFuture, i, key));
            }
            return responseFuture.get();
        }catch (Exception e){
            log.warn("upload file parts failed, cause: ", e);
            throw e;
        }
    }

    private List<Url> parseUploadUrls(byte[] content){
        String str = StringUtil.toString(content);
        String[] urls = str.split(";");
        List<Url> result = new ArrayList<>(urls.length);
        for (String url : urls) {
            result.add(Url.parseString(url));
        }
        return result;
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
