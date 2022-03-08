package com.jay.oss.proxy.service;

import com.jay.dove.DoveClient;
import com.jay.dove.transport.Url;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.entity.DeleteObjectInBucketRequest;
import com.jay.oss.common.entity.DeleteRequest;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.proxy.util.HttpUtil;
import io.netty.handler.codec.http.FullHttpResponse;
import lombok.extern.slf4j.Slf4j;

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
    public FullHttpResponse deleteObject(String key, String bucket, String token){
        FullHttpResponse httpResponse;
        try{
            // 发送删除存储桶内object请求，同时验证权限
            CommandCode code = deleteObjectInBucket(key, bucket, token);
            // 权限通过
            if(code.equals(FastOssProtocol.SUCCESS)){
                // 创建删除object数据请求
                DeleteRequest requestBody = DeleteRequest.builder().key(bucket + key).build();
                RemotingCommand request = client.getCommandFactory()
                        .createRequest(requestBody, FastOssProtocol.DELETE_OBJECT, DeleteRequest.class);
                Url url = Url.parseString("127.0.0.1:9999");
                // 同步发送
                FastOssCommand response = (FastOssCommand)client.sendSync(url, request, null);
                httpResponse = HttpUtil.okResponse();
            }else{
                httpResponse = HttpUtil.bucketAclResponse(code);
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
    private CommandCode deleteObjectInBucket(String key, String bucket, String token)throws Exception{
        Url url = Url.parseString(OssConfigs.trackerServerHost());
        DeleteObjectInBucketRequest request = DeleteObjectInBucketRequest.builder()
                .bucket(bucket).key(key).token(token).build();
        RemotingCommand command = client.getCommandFactory()
                .createRequest(request, FastOssProtocol.BUCKET_DELETE_OBJECT, DeleteObjectInBucketRequest.class);
        return client.sendSync(url, command, null).getCommandCode();
    }
}
