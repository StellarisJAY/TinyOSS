package com.jay.oss.proxy.service;

import com.jay.dove.DoveClient;
import com.jay.dove.transport.Url;
import com.jay.oss.common.acl.Acl;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.entity.Bucket;
import com.jay.oss.common.entity.ListBucketRequest;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.util.HttpUtil;
import com.jay.oss.common.util.SerializeUtil;
import io.netty.handler.codec.http.*;

import java.nio.charset.StandardCharsets;

/**
 * <p>
 *  Bucket请求处理器
 * </p>
 *
 * @author Jay
 * @date 2022/02/16 11:19
 */
public class BucketService {

    private final DoveClient client;

    public BucketService(DoveClient client) {
        this.client = client;
    }

    /**
     * put bucket
     * @param bucketName 桶名称
     * @param ownerId 创建者ID
     * @param acl 访问权限
     * @return {@link FullHttpResponse}
     */
    public FullHttpResponse putBucket(String bucketName, String ownerId, String acl){
        // 创建桶
        Bucket bucket = Bucket.builder()
                .bucketName(bucketName)
                .acl(Acl.getAcl(acl))
                .ownerId(ownerId)
                .build();
        // 序列化
        byte[] content = SerializeUtil.serialize(bucket, Bucket.class);
        // 创建请求
        FastOssCommand command = (FastOssCommand) client.getCommandFactory()
                .createRequest(content, FastOssProtocol.PUT_BUCKET);

        // 寻找目标storage地址
        Url url = Url.parseString(OssConfigs.trackerServerHost());

        FullHttpResponse httpResponse;
        try{
            // 发送请求
            FastOssCommand response = (FastOssCommand) client.sendSync(url, command, null);
            String keyPair = new String(response.getContent(), StandardCharsets.UTF_8);
            String[] keyPairs = keyPair.split(";");
            String appId = keyPairs[0];
            String accessKey = keyPairs[1];
            String secretKey = keyPairs[2];

            httpResponse = HttpUtil.okResponse();
            HttpHeaders headers = httpResponse.headers();
            headers.add("foss-AccessKey", accessKey);
            headers.add("foss-SecretKey", secretKey);
            headers.add("foss-AppId", appId);
        }catch (Exception e){
            httpResponse = HttpUtil.internalErrorResponse("Internal Server Error");
        }
        return httpResponse;
    }

    /**
     * list 桶内对象
     * @param bucket 桶
     * @param token 访问token
     * @param count list数量
     * @param offset 偏移量
     * @return {@link FullHttpResponse}
     */
    public FullHttpResponse listBucket(String bucket, String token, int count, int offset){
        ListBucketRequest request = new ListBucketRequest(bucket, token, count, offset);
        // 序列化请求
        byte[] content = SerializeUtil.serialize(request, ListBucketRequest.class);
        FastOssCommand command = (FastOssCommand)client.getCommandFactory()
                .createRequest(content, FastOssProtocol.LIST_BUCKET);
        // 查询目标storage
        Url url = Url.parseString(OssConfigs.trackerServerHost());

        FullHttpResponse httpResponse;
        try{
            // 发送List请求
            FastOssCommand response = (FastOssCommand) client.sendSync(url, command, null);
            httpResponse = HttpUtil.okResponse();
            httpResponse.content().writeBytes(response.getContent());
        }catch (Exception e){
            httpResponse = HttpUtil.internalErrorResponse("Internal Server Error");
        }
        return httpResponse;
    }
}
