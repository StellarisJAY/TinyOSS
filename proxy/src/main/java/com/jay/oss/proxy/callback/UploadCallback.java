package com.jay.oss.proxy.callback;

import com.jay.dove.transport.callback.InvokeCallback;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/03/10 15:32
 */
@Slf4j
public class UploadCallback implements InvokeCallback {
    private final CompletableFuture<FastOssCommand> responseFuture;
    private final int partNum;
    private String key;
    public UploadCallback(CompletableFuture<FastOssCommand> responseFuture, int partNum, String key) {
        this.responseFuture = responseFuture;
        this.partNum = partNum;
        this.key = key;
    }

    public UploadCallback(CompletableFuture<FastOssCommand> responseFuture, int partNum) {
        this.responseFuture = responseFuture;
        this.partNum = partNum;
        this.key = null;
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
