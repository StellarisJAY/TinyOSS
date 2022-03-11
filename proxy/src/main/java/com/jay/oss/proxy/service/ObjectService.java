package com.jay.oss.proxy.service;

import com.jay.dove.DoveClient;
import com.jay.dove.transport.Url;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.entity.DeleteObjectInBucketRequest;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.util.KeyUtil;
import com.jay.oss.common.util.StringUtil;
import com.jay.oss.common.util.UrlUtil;
import com.jay.oss.proxy.callback.AsyncBatchCallback;
import com.jay.oss.proxy.util.HttpUtil;
import io.netty.handler.codec.http.FullHttpResponse;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * <p>
 *  Object 服务
 * </p>
 *
 * @author Jay
 * @date 2022/02/12 12:47
 */
@Slf4j
public class ObjectService {
    private final DoveClient client;

    public ObjectService(DoveClient client) {
        this.client = client;
    }

    /**
     * delete object
     * @param key object key
     * @param bucket bucket
     * @param token token
     * @return {@link FullHttpResponse}
     */
    public FullHttpResponse deleteObject(String key, String bucket, String versionId, String token){
        String objectKey = KeyUtil.getObjectKey(key, bucket, versionId);
        FullHttpResponse httpResponse;
        try{
            // 从Tracker删除object记录，同时验证权限，并返回Storages集合
            FastOssCommand response = deleteObjectInTracker(objectKey, bucket, token);
            CommandCode code = response.getCommandCode();
            // 权限通过
            if(code.equals(FastOssProtocol.SUCCESS)){
                List<Url> urls = UrlUtil.parseUrls(StringUtil.toString(response.getContent()));
                httpResponse = deleteObjectInStorages(urls, objectKey);
            }else{
                httpResponse = HttpUtil.errorResponse(code);
            }
        }catch (Exception e){
            httpResponse = HttpUtil.internalErrorResponse("Internal server error");
        }
        return httpResponse;
    }

    /**
     * 删除桶内object记录
     * @param key key
     * @param bucket bucket
     * @param token token
     * @return {@link CommandCode} 删除状态码
     * @throws Exception e
     */
    private FastOssCommand deleteObjectInTracker(String key, String bucket, String token)throws Exception{
        Url url = OssConfigs.trackerServerUrl();
        DeleteObjectInBucketRequest request = DeleteObjectInBucketRequest.builder()
                .bucket(bucket).objectKey(key)
                .token(token)
                .build();
        RemotingCommand command = client.getCommandFactory()
                .createRequest(request, FastOssProtocol.DELETE_OBJECT, DeleteObjectInBucketRequest.class);
        return (FastOssCommand) client.sendSync(url, command, null);
    }


    private FullHttpResponse deleteObjectInStorages(List<Url> urls, String objectKey) throws InterruptedException {
        RemotingCommand command = client.getCommandFactory()
                .createRequest(objectKey, FastOssProtocol.DELETE_OBJECT);
        CountDownLatch countDownLatch = new CountDownLatch(urls.size());
        List<Url> successUrls = new ArrayList<>();
        List<Url> failedUrls = new ArrayList<>();
        for (Url url : urls) {
            client.sendAsync(url, command, new AsyncBatchCallback(countDownLatch, successUrls, failedUrls, url));
        }
        countDownLatch.await();
        if(successUrls.size() > 0){
            return HttpUtil.okResponse();
        }else{
            return HttpUtil.internalErrorResponse("Delete Object In Storages Failed");
        }
    }
}
