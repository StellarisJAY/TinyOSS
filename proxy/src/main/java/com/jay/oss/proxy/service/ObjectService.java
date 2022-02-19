package com.jay.oss.proxy.service;

import com.jay.dove.DoveClient;
import com.jay.dove.serialize.Serializer;
import com.jay.dove.serialize.SerializerManager;
import com.jay.dove.transport.Url;
import com.jay.dove.transport.command.CommandCode;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.entity.DeleteObjectInBucketRequest;
import com.jay.oss.common.entity.DeleteRequest;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.util.SerializeUtil;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 *
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
        DefaultFullHttpResponse httpResponse;
        try{
            // 发送删除存储桶内object请求，同时验证权限
            CommandCode code = deleteObjectInBucket(key, bucket, token);
            // 权限通过
            if(code.equals(FastOssProtocol.SUCCESS)){
                // 创建删除object数据请求
                DeleteRequest requestBody = DeleteRequest.builder().key(bucket + key).build();
                byte[] content = SerializeUtil.serialize(requestBody, DeleteRequest.class);
                FastOssCommand request = (FastOssCommand)client.getCommandFactory()
                        .createRequest(content, FastOssProtocol.DELETE_OBJECT);
                Url url = Url.parseString("127.0.0.1:9999?conn=10");
                // 同步发送
                FastOssCommand response = (FastOssCommand)client.sendSync(url, request, null);
                httpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
            }else if(code.equals(FastOssProtocol.ACCESS_DENIED)){
                // 无访问权限
                httpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.UNAUTHORIZED);
            }else{
                // 桶或者object不存在
                httpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND);
            }
        }catch (Exception e){
            httpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
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
        Url url = Url.parseString("127.0.0.1:9999");
        DeleteObjectInBucketRequest request = DeleteObjectInBucketRequest.builder()
                .bucket(bucket).key(key).token(token).build();
        byte[] content = SerializeUtil.serialize(request, DeleteObjectInBucketRequest.class);
        FastOssCommand command = (FastOssCommand) client.getCommandFactory()
                .createRequest(content, FastOssProtocol.BUCKET_DELETE_OBJECT);
        return client.sendSync(url, command, null).getCommandCode();
    }
}
