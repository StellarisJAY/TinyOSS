package com.jay.oss.proxy.callback;

import com.jay.dove.transport.Url;
import com.jay.dove.transport.callback.InvokeCallback;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.remoting.FastOssProtocol;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

/**
 * <p>
 *  异步批量发送Callback
 * </p>
 *
 * @author Jay
 * @date 2022/03/11 10:51
 */
public class AsyncBatchCallback implements InvokeCallback {

    private final CountDownLatch countDownLatch;
    private final List<Url> successUrls;
    private final List<Url> failedUrls;
    private final Url url;
    public AsyncBatchCallback(CountDownLatch countDownLatch, List<Url> successUrls, List<Url> failedUrls, Url url){
        this.countDownLatch = countDownLatch;
        this.successUrls = successUrls;
        this.failedUrls = failedUrls;
        this.url = url;
    }

    @Override
    public void onComplete(RemotingCommand remotingCommand) {
        CommandCode code = remotingCommand.getCommandCode();
        if(code.equals(FastOssProtocol.SUCCESS)){
            successUrls.add(url);
        }else{
            failedUrls.add(url);
        }
        countDownLatch.countDown();
    }

    @Override
    public void exceptionCaught(Throwable throwable) {
        failedUrls.add(url);
        countDownLatch.countDown();
    }

    @Override
    public void onTimeout(RemotingCommand remotingCommand) {
        failedUrls.add(url);
        countDownLatch.countDown();
    }

    @Override
    public ExecutorService getExecutor() {
        return null;
    }
}
