package com.jay.oss.proxy.service;

import com.jay.dove.DoveClient;
import com.jay.dove.transport.Url;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.acl.BucketAccessMode;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.entity.request.GetObjectRequest;
import com.jay.oss.common.entity.request.LocateObjectRequest;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.util.KeyUtil;
import com.jay.oss.common.util.StringUtil;
import com.jay.oss.common.util.UrlUtil;
import com.jay.oss.proxy.util.HttpUtil;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;

/**
 * <p>
 *  下载服务
 * </p>
 *
 * @author Jay
 * @date 2022/02/11 11:05
 */
@Slf4j
public class DownloadService {
    private final DoveClient client;
    public DownloadService(DoveClient client) {
        this.client = client;
    }

    /**
     * 获取对象，range表示获取对象的数据的范围
     * @param key key
     * @param bucket 桶
     * @param token AccessToken
     * @param rangeStart 起始字节
     * @param rangeEnd 结束字节
     * @return {@link FullHttpResponse}
     */
    public FullHttpResponse getObject(String key, String bucket, String token, String versionId, int rangeStart, int rangeEnd){
        if(StringUtil.isNullOrEmpty(key) || StringUtil.isNullOrEmpty(bucket)){
            return HttpUtil.badRequestResponse("Missing important parameters for Get Object");
        }
        String objectKey = KeyUtil.getObjectKey(key, bucket, versionId);
        // 根据范围判断下载类型，full或者ranged
        CommandCode commandCode = rangeEnd == -1 ? FastOssProtocol.DOWNLOAD_FULL : FastOssProtocol.DOWNLOAD_RANGED;

        try{
            // 向Tracker服务器定位Object位置
            FastOssCommand locateResponse = locateObject(objectKey, bucket, token);
            CommandCode code = locateResponse.getCommandCode();
            if(!code.equals(FastOssProtocol.SUCCESS)){
                return HttpUtil.bucketAclResponse(code);
            }
            String respContent = StringUtil.toString(locateResponse.getContent());
            String[] split = respContent.split(";");
            int replicaCount = OssConfigs.replicaCount();
            if(split.length <= replicaCount){
                return HttpUtil.internalErrorResponse("Internal Server Error");
            }
            long objectId = Long.parseLong(split[replicaCount]);
            List<Url> urls = UrlUtil.parseUrls(split, replicaCount);
            // 尝试从url列表中下载object
            return tryDownload(urls, objectId, rangeStart, rangeEnd, commandCode);
        }catch (Exception e){
            log.error("download service error: ", e);
            return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }

    public FastOssCommand locateObject(String key, String bucket, String token) throws Exception {
        Url url = OssConfigs.trackerServerUrl();
        LocateObjectRequest request = new LocateObjectRequest(key, bucket, token, BucketAccessMode.READ);
        RemotingCommand command = client.getCommandFactory()
                .createRequest(request, FastOssProtocol.LOCATE_OBJECT, LocateObjectRequest.class);
        // 同步发送
        return (FastOssCommand)client.sendSync(url, command, null);
    }


    private FullHttpResponse tryDownload(List<Url> urls, long objectId, int start, int end, CommandCode code){
        GetObjectRequest request = new GetObjectRequest(objectId, start, end);
        RemotingCommand command = client.getCommandFactory().createRequest(request, code, GetObjectRequest.class);
        Collections.shuffle(urls);
        for (Url url : urls) {
            try{
                // 发送下载请求
                FastOssCommand response = (FastOssCommand)client.sendSync(url, command, null);
                CommandCode respCode = response.getCommandCode();
                if(respCode.equals(FastOssProtocol.DOWNLOAD_RESPONSE)){
                    // 全量下载返回 200OK
                    if(end == -1){
                        return HttpUtil.okResponse(response.getData());
                    }else{
                        // 部分下载返回206 Partial Content
                        return HttpUtil.partialContentResponse(response.getData());
                    }
                }
            }catch (Exception e){
                log.warn("Failed to Get Object From: {}", url, e);
            }
        }
        return HttpUtil.notFoundResponse("Object Not Found");
    }
}
