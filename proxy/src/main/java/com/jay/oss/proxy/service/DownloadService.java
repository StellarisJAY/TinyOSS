package com.jay.oss.proxy.service;

import com.jay.dove.DoveClient;
import com.jay.dove.transport.Url;
import com.jay.dove.transport.command.CommandCode;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.entity.DownloadRequest;
import com.jay.oss.common.entity.LocateObjectRequest;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.util.StringUtil;
import com.jay.oss.proxy.cache.CacheValue;
import com.jay.oss.proxy.cache.ObjectLocationCache;
import com.jay.oss.proxy.util.HttpUtil;
import com.jay.oss.common.util.SerializeUtil;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import lombok.extern.slf4j.Slf4j;

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
        String objectKey = bucket + "/" + key + (StringUtil.isNullOrEmpty(versionId) ? "" : "-" + versionId);
        log.info("Get Object: {}", objectKey);
        // 根据范围判断下载类型，full或者ranged
        CommandCode commandCode = rangeEnd == -1 ? FastOssProtocol.DOWNLOAD_FULL : FastOssProtocol.DOWNLOAD_RANGED;
        // 创建下载请求
        DownloadRequest request = new DownloadRequest(objectKey, rangeEnd == -1, rangeStart, rangeEnd);
        // 序列化
        byte[] serialized = SerializeUtil.serialize(request, DownloadRequest.class);
        // 创建command
        FastOssCommand command = (FastOssCommand)client.getCommandFactory().
                createRequest(serialized, commandCode);

        ByteBuf content;
        try{
            FastOssCommand locateResponse = locateObject(objectKey, bucket, token);
            CommandCode code = locateResponse.getCommandCode();
            if(code.equals(FastOssProtocol.SUCCESS)){
                String respContent = new String(locateResponse.getContent(), OssConfigs.DEFAULT_CHARSET);
                String[] urls = respContent.split(";");
                Url url = Url.parseString(urls[0]);
                // 发送下载请求
                FastOssCommand response = (FastOssCommand)client.sendSync(url, command, null);
                // object不存在
                if(response.getCommandCode().equals(FastOssProtocol.OBJECT_NOT_FOUND)){
                    return HttpUtil.notFoundResponse("object not found");
                }
                content = response.getData();
                // 全量下载返回 200OK
                if(commandCode.equals(FastOssProtocol.DOWNLOAD_FULL)){
                    return HttpUtil.okResponse(content);
                }else{
                    // 部分下载返回206 Partial Content
                    return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.PARTIAL_CONTENT, content);
                }
            } else if(code.equals(FastOssProtocol.ACCESS_DENIED)){
                // 无访问权限
                return HttpUtil.forbiddenResponse("Access Denied");
            }
            else if(code.equals(FastOssProtocol.OBJECT_NOT_FOUND)){
                // 桶不存在
                return HttpUtil.notFoundResponse("Object Not Found");
            }
            else{
                return HttpUtil.notFoundResponse("Bucket Not Found");
            }

        }catch (Exception e){
            log.error("download service error: ", e);
            return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }

    public FastOssCommand locateObject(String key, String bucket, String token) throws Exception {
        Url url = Url.parseString(OssConfigs.trackerServerHost());
        LocateObjectRequest request = new LocateObjectRequest(key, bucket, token);
        byte[] content = SerializeUtil.serialize(request, LocateObjectRequest.class);
        FastOssCommand command = (FastOssCommand) client.getCommandFactory()
                .createRequest(content, FastOssProtocol.LOCATE_OBJECT);
        // 同步发送
        return (FastOssCommand)client.sendSync(url, command, null);
    }
}
