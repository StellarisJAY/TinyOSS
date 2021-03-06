package com.jay.oss.proxy.service;

import com.jay.dove.DoveClient;
import com.jay.dove.transport.Url;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.acl.BucketAccessMode;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.entity.request.GetObjectRequest;
import com.jay.oss.common.entity.request.LocateObjectRequest;
import com.jay.oss.common.entity.response.LocateObjectResponse;
import com.jay.oss.common.remoting.TinyOssCommand;
import com.jay.oss.common.remoting.TinyOssProtocol;
import com.jay.oss.common.util.KeyUtil;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.common.util.StringUtil;
import com.jay.oss.proxy.util.HttpUtil;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

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
        CommandCode commandCode = rangeEnd == -1 ? TinyOssProtocol.DOWNLOAD_FULL : TinyOssProtocol.DOWNLOAD_RANGED;

        try{
            // 向Tracker服务器定位Object位置
            TinyOssCommand locateResponse = locateObject(objectKey, bucket, token);
            CommandCode code = locateResponse.getCommandCode();
            if(!code.equals(TinyOssProtocol.SUCCESS)){
                return HttpUtil.bucketAclResponse(code);
            }
            LocateObjectResponse response = SerializeUtil.deserialize(locateResponse.getContent(), LocateObjectResponse.class);
            long objectId = response.getObjectId();
            Set<String> urls = response.getLocations();
            if(urls == null || urls.isEmpty()){
                return HttpUtil.notFoundResponse("No Replica of this object found");
            }
            // 尝试从url列表中下载object
            return tryDownload(new ArrayList<>(urls), objectId, rangeStart, rangeEnd, commandCode);
        }catch (Exception e){
            log.error("download service error: ", e);
            return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * 从Tracker获取object所在的存储服务器
     * @param key objectKey
     * @param bucket 存储桶
     * @param token 访问token
     * @return {@link TinyOssCommand}
     * @throws Exception e
     */
    public TinyOssCommand locateObject(String key, String bucket, String token) throws Exception {
        Url url = OssConfigs.trackerServerUrl();
        LocateObjectRequest request = new LocateObjectRequest(key, bucket, token, BucketAccessMode.READ);
        RemotingCommand command = client.getCommandFactory()
                .createRequest(request, TinyOssProtocol.LOCATE_OBJECT, LocateObjectRequest.class);
        // 同步发送
        return (TinyOssCommand)client.sendSync(url, command, null);
    }

    /**
     * 尝试从存储服务器下载对象
     * 默认会打乱存储服务器顺序，来简单地实现负载均衡
     * @param urls 存储服务器地址列表
     * @param objectId 对象ID
     * @param start 下载范围开始位置
     * @param end 下载范围结束位置
     * @param code {@link CommandCode}
     * @return {@link FullHttpResponse}
     */
    private FullHttpResponse tryDownload(List<String> urls, long objectId, int start, int end, CommandCode code){
        GetObjectRequest request = new GetObjectRequest(objectId, start, end);
        RemotingCommand command = client.getCommandFactory().createRequest(request, code, GetObjectRequest.class);
        Collections.shuffle(urls);
        for (String urlStr : urls) {
            Url url = Url.parseString(urlStr);
            try{
                // 发送下载请求
                TinyOssCommand response = (TinyOssCommand)client.sendSync(url, command, null);
                CommandCode respCode = response.getCommandCode();
                if(respCode.equals(TinyOssProtocol.DOWNLOAD_RESPONSE)){
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
