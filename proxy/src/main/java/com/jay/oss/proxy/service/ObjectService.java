package com.jay.oss.proxy.service;

import com.jay.dove.DoveClient;
import com.jay.dove.transport.Url;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.acl.BucketAccessMode;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.entity.request.DeleteObjectInBucketRequest;
import com.jay.oss.common.entity.request.LocateObjectRequest;
import com.jay.oss.common.entity.object.ObjectVO;
import com.jay.oss.common.remoting.TinyOssCommand;
import com.jay.oss.common.remoting.TinyOssProtocol;
import com.jay.oss.common.util.KeyUtil;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.common.util.StringUtil;
import com.jay.oss.proxy.entity.Result;
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
    public FullHttpResponse deleteObject(String key, String bucket, String versionId, String token){
        if(StringUtil.isNullOrEmpty(key) || StringUtil.isNullOrEmpty(bucket)){
            return HttpUtil.badRequestResponse("Missing important parameters for Delete Object");
        }
        String objectKey = KeyUtil.getObjectKey(key, bucket, versionId);
        FullHttpResponse httpResponse;
        try{
            // 从Tracker删除object记录，同时验证权限
            TinyOssCommand response = deleteObjectInTracker(objectKey, bucket, token);
            return HttpUtil.httpResponseOfCode(response.getCommandCode());
        }catch (Exception e){
            log.error("Delete Object Failed ", e);
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
    private TinyOssCommand deleteObjectInTracker(String key, String bucket, String token)throws Exception{
        Url url = OssConfigs.trackerServerUrl();
        DeleteObjectInBucketRequest request = DeleteObjectInBucketRequest.builder()
                .bucket(bucket).objectKey(key)
                .accessMode(BucketAccessMode.WRITE)
                .token(token)
                .build();
        RemotingCommand command = client.getCommandFactory()
                .createRequest(request, TinyOssProtocol.DELETE_OBJECT, DeleteObjectInBucketRequest.class);
        return (TinyOssCommand) client.sendSync(url, command, null);
    }

    public FullHttpResponse getObjectMeta(String key, String bucket, String version, String token){
        String objectKey = KeyUtil.getObjectKey(key, bucket, version);
        LocateObjectRequest request = new LocateObjectRequest(objectKey, bucket, token, BucketAccessMode.READ);
        Url trackerUrl = OssConfigs.trackerServerUrl();
        RemotingCommand command = client.getCommandFactory()
                .createRequest(request, TinyOssProtocol.GET_OBJECT_META, LocateObjectRequest.class);
        try{
            RemotingCommand response = client.sendSync(trackerUrl, command, null);
            CommandCode code = response.getCommandCode();
            if(TinyOssProtocol.SUCCESS.equals(code)){
                ObjectVO vo = SerializeUtil.deserialize(response.getContent(), ObjectVO.class);
                Result result = new Result().message("Success")
                        .putData("meta", vo);
                return HttpUtil.okResponse(result);
            }else{
                return HttpUtil.errorResponse(code);
            }
        }catch (Exception e){
            return HttpUtil.internalErrorResponse("Internal Server Error");
        }
    }
}
