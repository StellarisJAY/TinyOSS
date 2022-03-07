package com.jay.oss.proxy.service;

import com.jay.dove.DoveClient;
import com.jay.dove.transport.Url;
import com.jay.dove.transport.command.CommandCode;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.entity.BucketPutObjectRequest;
import com.jay.oss.common.entity.LookupMultipartUploadRequest;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.common.util.StringUtil;
import com.jay.oss.common.util.UrlUtil;
import com.jay.oss.proxy.entity.Result;
import com.jay.oss.proxy.util.HttpUtil;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpResponse;

import java.util.List;

/**
 * <p>
 *  Multipart Upload Service
 * </p>
 *
 * @author Jay
 * @date 2022/03/07 14:10
 */
public class MultipartUploadService {

    private final DoveClient client;

    public MultipartUploadService(DoveClient client) {
        this.client = client;
    }

    public FullHttpResponse initializeMultipartUpload(String key, String bucket, String token, int size){
        String objectKey = bucket + "/" + key;
        BucketPutObjectRequest request = BucketPutObjectRequest.builder()
                .bucket(bucket).key(objectKey).filename(key)
                .token(token).size(size).createTime(System.currentTimeMillis()).build();

        byte[] content = SerializeUtil.serialize(request, BucketPutObjectRequest.class);
        FastOssCommand command = (FastOssCommand) client.getCommandFactory()
                .createRequest(content, FastOssProtocol.INIT_MULTIPART_UPLOAD);

        String host = OssConfigs.trackerServerHost();
        Url url = Url.parseString(host);
        FullHttpResponse httpResponse;
        try {
            // 发送INIT_MULTIPART 请求
            FastOssCommand response = (FastOssCommand) client.sendSync(url, command, null);
            CommandCode code = response.getCommandCode();
            if(code.equals(FastOssProtocol.SUCCESS)){
                // 获取返回的uploadID
                String uploadId = new String(response.getContent(), OssConfigs.DEFAULT_CHARSET);
                Result result = new Result().message("Multipart Upload Initialized").putData("UploadId", uploadId);
                httpResponse =  HttpUtil.okResponse(result);
            }else {
                // bucket 权限response
                httpResponse = HttpUtil.bucketAclResponse(code);
            }
        } catch (InterruptedException e) {
            httpResponse = HttpUtil.internalErrorResponse("Internal Server Error " +  e);
        }
        return httpResponse;
    }

    public FullHttpResponse putObject(String key, String bucket, String token, String uploadId, ByteBuf content){
        String objectKey = bucket + "/" + key;

        FullHttpResponse httpResponse;
        try{
            // 用uploadId查询分片上传任务的storages
            FastOssCommand lookupResponse = lookupMultipartRequest(objectKey, uploadId, bucket, token);
            CommandCode code = lookupResponse.getCommandCode();
            if(FastOssProtocol.SUCCESS.equals(code)){
                // 查询到的目标storages
                List<Url> urls = UrlUtil.parseUrls(StringUtil.toString(lookupResponse.getContent()));
                httpResponse = HttpUtil.okResponse();
            }else{
                // 存储桶访问权限response
                httpResponse = HttpUtil.bucketAclResponse(code);
            }
        }catch (Exception e){
            httpResponse = HttpUtil.internalErrorResponse("Internal Server Error " +  e);
        }
        return httpResponse;
    }

    /**
     * 查询multipart上传信息
     * @param objectKey key
     * @param uploadId uploadID
     * @param bucket 存储桶
     * @param token token
     * @return {@link FastOssCommand}
     * @throws InterruptedException e
     */
    private FastOssCommand lookupMultipartRequest(String objectKey, String uploadId, String bucket, String token) throws InterruptedException {
        String host = OssConfigs.trackerServerHost();
        Url url = Url.parseString(host);
        LookupMultipartUploadRequest request = new LookupMultipartUploadRequest(uploadId, objectKey, bucket, token);
        byte[] content = SerializeUtil.serialize(request, LookupMultipartUploadRequest.class);
        FastOssCommand command = (FastOssCommand) client.getCommandFactory()
                .createRequest(content, FastOssProtocol.LOOKUP_MULTIPART_UPLOAD);
        return (FastOssCommand) client.sendSync(url, command, null);
    }
}
