package com.jay.oss.proxy.service;

import com.jay.dove.DoveClient;
import com.jay.dove.transport.Url;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.entity.BucketPutObjectRequest;
import com.jay.oss.common.entity.LookupMultipartUploadRequest;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
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

    /**
     * 初始化Multipart上传
     * 向Tracker申请开启一个分片上传任务，同时校验存储桶访问权限
     * 请求完成后会返回一个UploadID作为分片上传的标识
     * @param key object Key
     * @param bucket bucket
     * @param token token
     * @param size object size
     * @return {@link FullHttpResponse}
     */
    public FullHttpResponse initializeMultipartUpload(String key, String bucket, String token, int size){
        String objectKey = bucket + "/" + key;
        // Bucket Put Object请求
        BucketPutObjectRequest request = BucketPutObjectRequest.builder()
                .bucket(bucket).key(objectKey).filename(key)
                .token(token).size(size).createTime(System.currentTimeMillis()).build();

        RemotingCommand command = client.getCommandFactory()
                .createRequest(request, FastOssProtocol.INIT_MULTIPART_UPLOAD, BucketPutObjectRequest.class);

        Url url = OssConfigs.trackerServerUrl();
        FullHttpResponse httpResponse;
        try {
            // 发送INIT_MULTIPART 请求
            FastOssCommand response = (FastOssCommand) client.sendSync(url, command, null);
            CommandCode code = response.getCommandCode();
            if(code.equals(FastOssProtocol.SUCCESS)){
                // 获取返回的uploadID
                String content = new String(response.getContent(), OssConfigs.DEFAULT_CHARSET);
                String[] split = content.split(";");
                Result result = new Result().message("Multipart Upload Initialized")
                        .putData("uploadId", split[0])
                        .putData("versionId", split[1]);
                httpResponse =  HttpUtil.okResponse(result);
            } else {
                // bucket 权限response
                httpResponse = HttpUtil.bucketAclResponse(code);
            }
        } catch (InterruptedException e) {
            httpResponse = HttpUtil.internalErrorResponse("Internal Server Error " +  e);
        }
        return httpResponse;
    }

    /**
     * 上传object分片
     * @param key object key
     * @param bucket 存储桶
     * @param token 访问token
     * @param uploadId 上传任务ID
     * @param content 分片数据
     * @return {@link FullHttpResponse}
     */
    public FullHttpResponse putObject(String key, String bucket, String token, String uploadId, String versionId, int partNum, ByteBuf content){
        String objectKey = bucket + "/" + key + (StringUtil.isNullOrEmpty(versionId) ? "" : "-" + versionId);
        FullHttpResponse httpResponse;
        try{
            // 用uploadId查询分片上传任务的storages
            FastOssCommand lookupResponse = lookupMultipartRequest(objectKey, uploadId, bucket, token);
            CommandCode code = lookupResponse.getCommandCode();
            if(FastOssProtocol.SUCCESS.equals(code)){
                // 查询到的目标storages
                List<Url> urls = UrlUtil.parseUrls(StringUtil.toString(lookupResponse.getContent()));

                httpResponse = HttpUtil.okResponse(urls.toString());
            }else if(FastOssProtocol.MULTIPART_UPLOAD_FINISHED.equals(code)){
                httpResponse = HttpUtil.badRequestResponse("Multi-part Upload Task already finished");
            } else{
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
        Url url = OssConfigs.trackerServerUrl();
        LookupMultipartUploadRequest request = new LookupMultipartUploadRequest(uploadId, objectKey, bucket, token);
        RemotingCommand command = client.getCommandFactory()
                .createRequest(request, FastOssProtocol.LOOKUP_MULTIPART_UPLOAD, LookupMultipartUploadRequest.class);
        return (FastOssCommand) client.sendSync(url, command, null);
    }
}
