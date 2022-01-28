package com.jay.oss.proxy.service;

import com.jay.dove.DoveClient;
import com.jay.dove.config.Configs;
import com.jay.dove.serialize.Serializer;
import com.jay.dove.serialize.SerializerManager;
import com.jay.dove.transport.Url;
import com.jay.dove.transport.callback.InvokeCallback;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.OssConfigs;
import com.jay.oss.common.entity.FilePart;
import com.jay.oss.common.entity.UploadRequest;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;

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


    public void putObject(String key, String bucket, String auth, ByteBuf content) throws Exception {
        // 检查桶是否存在，以及是否有访问权限
        if(checkBucket(bucket, auth)){
            long size = content.readableBytes();
            // 计算分片个数
            int parts = (int)(size / FilePart.DEFAULT_PART_SIZE + (int)(size % FilePart.DEFAULT_PART_SIZE == 0 ? 0 : 1));
            // 创建上传请求
            UploadRequest request = UploadRequest.builder()
                    .key(key)
                    .bucket(bucket)
                    .filename(key)
                    .ownerId(auth)
                    .size(size)
                    .parts(parts)
                    .build();

            // 目标存储节点地址，从一致性HASH获取
            Url url = Url.parseString("127.0.0.1:9999?conn=20");
            // 向存储节点发送文件信息，开启上传任务
            FastOssCommand uploadHeaderResponse = uploadFileHeader(url, request);
            if(uploadHeaderResponse == null){
                log.warn("null response");
                return;
            }
            CommandCode responseCode = uploadHeaderResponse.getCommandCode();
            byte[] responseContent = uploadHeaderResponse.getContent();
            if(FastOssProtocol.ERROR.equals(responseCode)){
                // 上传出现错误 或
                log.warn("upload header failed, {}", new String(responseContent, Configs.DEFAULT_CHARSET));
            }else if (FastOssProtocol.REQUEST_TIMEOUT.equals(responseCode)){
                // 上传超时
                log.warn("upload header timeout, {}", new String(responseContent, Configs.DEFAULT_CHARSET));
            }
            else{
                // 上传文件分片
                FastOssCommand response = uploadFileParts(url, content, size, parts, key);
            }
        }else{
            // 没有上传权限
            throw new IllegalAccessException("can't put object into bucket, access denied.");
        }
    }

    public boolean checkBucket(String bucket, String auth){
        // search for bucket acl and check auth
        return true;
    }

    private FastOssCommand uploadFileHeader(Url url, UploadRequest request) throws InterruptedException {
        Serializer serializer = SerializerManager.getSerializer(OssConfigs.DEFAULT_SERIALIZER);
        byte[] content = serializer.serialize(request, UploadRequest.class);
        RemotingCommand command = storageClient.getCommandFactory().createRequest(content, FastOssProtocol.UPLOAD_FILE_HEADER);
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
        byte[] keyBytes = key.getBytes(Configs.DEFAULT_CHARSET);
        // 所有分片共享的头信息
        ByteBuf headBuffer = Unpooled.directBuffer(FilePart.DEFAULT_PART_SIZE + 4 + keyBytes.length);
        try{
            // 写入key长度
            headBuffer.writeInt(keyBytes.length);
            // 写入key
            headBuffer.writeBytes(keyBytes);
            // future, 等待所有分片上传完成
            CompletableFuture<FastOssCommand> responseFuture = new CompletableFuture<>();
            for(int i = 0; i < parts; i++){
                ByteBuf fullBuffer = headBuffer.copy();
                // 写入分片编号
                fullBuffer.writeInt(i);
                // buffer写入一个分片
                fullBuffer.writeBytes(content, i == parts - 1 ? content.readableBytes() : FilePart.DEFAULT_PART_SIZE);
                // 创建请求
                FastOssCommand request = (FastOssCommand)commandFactory.createRequest(fullBuffer, FastOssProtocol.UPLOAD_FILE_PARTS);
                // 发送文件分片，异步方式发送
                storageClient.sendAsync(url, request, new UploadCallback(responseFuture, i, key));
            }
            return responseFuture.get();
        }catch (Exception e){
            log.warn("upload file parts failed, cause: ", e);
            throw e;
        } finally {
            // 释放buffer
            headBuffer.release();
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
