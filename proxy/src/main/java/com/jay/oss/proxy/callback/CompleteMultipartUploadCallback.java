package com.jay.oss.proxy.callback;

import com.jay.dove.transport.Url;
import com.jay.dove.transport.callback.InvokeCallback;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.remoting.TinyOssProtocol;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/03/10 15:26
 */
public class CompleteMultipartUploadCallback implements InvokeCallback {
    private List<Url> successUrls;
    private List<Url> asyncBackupUrls;
    private Url url;
    private CountDownLatch countDownLatch;

    public CompleteMultipartUploadCallback(Url url, List<Url> successUrls, List<Url> asyncBackupUrls, CountDownLatch countDownLatch) {
        this.successUrls = successUrls;
        this.asyncBackupUrls = asyncBackupUrls;
        this.url = url;
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void onComplete(RemotingCommand remotingCommand) {
        CommandCode code = remotingCommand.getCommandCode();
        if(TinyOssProtocol.SUCCESS.equals(code)){
            successUrls.add(url);
        }else{
            asyncBackupUrls.add(url);
        }
        countDownLatch.countDown();
    }

    @Override
    public void exceptionCaught(Throwable throwable) {
        asyncBackupUrls.add(url);
        countDownLatch.countDown();
    }

    @Override
    public void onTimeout(RemotingCommand remotingCommand) {
        asyncBackupUrls.add(url);
        countDownLatch.countDown();
    }

    @Override
    public ExecutorService getExecutor() {
        return null;
    }
}
