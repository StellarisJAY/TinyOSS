package com.jay.oss.proxy.service;

import com.alibaba.fastjson.JSON;
import com.jay.dove.DoveClient;
import com.jay.dove.transport.Url;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.acl.Acl;
import com.jay.oss.common.acl.BucketAccessMode;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.entity.bucket.Bucket;
import com.jay.oss.common.entity.bucket.UpdateAclRequest;
import com.jay.oss.common.entity.request.ListBucketRequest;
import com.jay.oss.common.entity.bucket.GetServiceRequest;
import com.jay.oss.common.entity.bucket.GetServiceResponse;
import com.jay.oss.common.remoting.TinyOssCommand;
import com.jay.oss.common.remoting.TinyOssProtocol;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.common.util.StringUtil;
import com.jay.oss.proxy.entity.Result;
import com.jay.oss.proxy.util.HttpUtil;
import io.netty.handler.codec.http.FullHttpResponse;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * <p>
 *  Bucket请求处理器
 * </p>
 *
 * @author Jay
 * @date 2022/02/16 11:19
 */
@Slf4j
public class BucketService {

    private final DoveClient client;

    public BucketService(DoveClient client) {
        this.client = client;
    }

    /**
     * put bucket
     * @param bucketName 桶名称
     * @param acl 访问权限
     * @return {@link FullHttpResponse}
     */
    public FullHttpResponse putBucket(String bucketName, String acl, boolean versioning){
        // 创建桶
        Bucket bucket = Bucket.builder()
                .bucketName(bucketName).acl(Acl.getAcl(acl))
                .versioning(versioning)
                .build();
        RemotingCommand command = client.getCommandFactory()
                .createRequest(bucket, TinyOssProtocol.PUT_BUCKET, Bucket.class);
        // 获取Tracker地址
        Url url = OssConfigs.trackerServerUrl();
        FullHttpResponse httpResponse;
        try{
            // 发送请求
            TinyOssCommand response = (TinyOssCommand) client.sendSync(url, command, null);
            String keyPair = new String(response.getContent(), StandardCharsets.UTF_8);
            String[] keyPairs = keyPair.split(";");
            String appId = keyPairs[0];
            String accessKey = keyPairs[1];
            String secretKey = keyPairs[2];

            Result result = new Result()
                    .message("Success")
                    .putData("toss-AccessKey", accessKey)
                    .putData("toss-SecretKey", secretKey)
                    .putData("toss-AppId", appId);
            httpResponse = HttpUtil.okResponse(result);
        }catch (Exception e){
            log.warn("Put Bucket Error ", e);
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
        ListBucketRequest request = new ListBucketRequest(bucket, token, count, offset, BucketAccessMode.READ);
        RemotingCommand command = client.getCommandFactory()
                .createRequest(request, TinyOssProtocol.LIST_BUCKET, ListBucketRequest.class);
        // 查询目标storage
        Url url = Url.parseString(OssConfigs.trackerServerHost());

        FullHttpResponse httpResponse;
        try{
            // 发送List请求
            TinyOssCommand response = (TinyOssCommand) client.sendSync(url, command, null);
            CommandCode code = response.getCommandCode();
            if(TinyOssProtocol.SUCCESS.equals(code)){
                byte[] content = response.getContent();
                String json = StringUtil.toString(content);
                List<String> list = JSON.parseArray(json, String.class);
                Result result = new Result().message("Success")
                        .putData("objects", list);
                httpResponse = HttpUtil.okResponse(result);
            }else{
                httpResponse = HttpUtil.errorResponse(code);
            }
        }catch (Exception e){
            log.error("List Bucket Error: ", e);
            httpResponse = HttpUtil.internalErrorResponse("Internal Server Error");
        }
        return httpResponse;
    }

    public FullHttpResponse getService(int page, int pageSize){
        GetServiceRequest request = new GetServiceRequest((page - 1) * pageSize, pageSize);
        Url url = OssConfigs.trackerServerUrl();
        try{
            RemotingCommand command = client.getCommandFactory()
                    .createRequest(request, TinyOssProtocol.GET_SERVICE, GetServiceRequest.class);
            RemotingCommand response = client.sendSync(url, command, null);
            GetServiceResponse getServiceResponse = SerializeUtil.deserialize(response.getContent(), GetServiceResponse.class);

            Result result = new Result()
                    .message("Success")
                    .putData("buckets", getServiceResponse.getBuckets())
                    .putData("total", getServiceResponse.getTotal());
            return HttpUtil.okResponse(result);
        }catch (Exception e){
            return HttpUtil.internalErrorResponse("Internal Server Error " + e);
        }
    }

    /**
     * 更新存储桶访问权限
     * @param bucket 存储桶名称
     * @param acl 修改后的权限
     * @param token token
     * @return {@link FullHttpResponse}
     */
    public FullHttpResponse updateBucketAcl(String bucket, String acl, String token){
        Acl targetAcl;
        // 参数校验
        if(StringUtil.isNullOrEmpty(bucket) || StringUtil.isNullOrEmpty(acl) || StringUtil.isNullOrEmpty(token)){
            return HttpUtil.badRequestResponse("Missing parameters for Update Acl Request");
        }
        if((targetAcl = Acl.getAcl(acl)) == null){
            return HttpUtil.badRequestResponse("Invalid Bucket ACL");
        }
        // 创建请求
        UpdateAclRequest request = new UpdateAclRequest(bucket, targetAcl, token, BucketAccessMode.WRITE_ACL);
        Url url = OssConfigs.trackerServerUrl();
        try{
            RemotingCommand command = client.getCommandFactory()
                    .createRequest(request, TinyOssProtocol.UPDATE_BUCKET_ACL, UpdateAclRequest.class);
            RemotingCommand response = client.sendSync(url, command, null);
            if(response.getCommandCode().equals(TinyOssProtocol.SUCCESS)){
                return HttpUtil.okResponse("Success");
            }else{
                return HttpUtil.errorResponse(response.getCommandCode());
            }
        }catch (Exception e){
            return HttpUtil.internalErrorResponse("Internal Server Error " + e);
        }
    }
}
