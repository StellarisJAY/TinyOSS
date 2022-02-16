package com.jay.oss.proxy.service;

import com.jay.dove.DoveClient;
import com.jay.dove.transport.Url;
import com.jay.oss.common.entity.Bucket;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
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
                .acl(acl)
                .ownerId(ownerId)
                .build();
        // 序列化
        byte[] content = SerializeUtil.serialize(bucket, Bucket.class);
        // 创建请求
        FastOssCommand command = (FastOssCommand) client.getCommandFactory()
                .createRequest(content, FastOssProtocol.PUT_BUCKET);

        // 寻找目标storage地址
        Url url = Url.parseString("127.0.0.1:9999");

        FullHttpResponse httpResponse;
        try{
            // 发送请求
            FastOssCommand response = (FastOssCommand) client.sendSync(url, command, null);
            String keyPair = new String(response.getContent(), StandardCharsets.UTF_8);
            String[] keyPairs = keyPair.split(";");
            String appId = keyPairs[0];
            String accessKey = keyPairs[1];
            String secretKey = keyPairs[2];

            httpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
            HttpHeaders headers = httpResponse.headers();
            headers.add("foss-AccessKey", accessKey);
            headers.add("foss-SecretKey", secretKey);
            headers.add("foss-AppId", appId);
        }catch (Exception e){
            httpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
        return httpResponse;
    }
}
