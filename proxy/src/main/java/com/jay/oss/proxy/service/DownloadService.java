package com.jay.oss.proxy.service;

import com.jay.dove.DoveClient;
import com.jay.dove.serialize.Serializer;
import com.jay.dove.serialize.SerializerManager;
import com.jay.dove.transport.Url;
import com.jay.dove.transport.command.CommandCode;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.entity.DownloadRequest;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
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
     * @param rangeStart 起始字节
     * @param rangeEnd 结束字节
     * @return {@link FullHttpResponse}
     */
    public FullHttpResponse getObject(String key, String bucket, int rangeStart, int rangeEnd){
        CommandCode commandCode = rangeEnd == -1 ? FastOssProtocol.DOWNLOAD_FULL : FastOssProtocol.DOWNLOAD_RANGED;

        DownloadRequest request = new DownloadRequest(bucket + key, rangeEnd == -1, rangeStart, rangeEnd);
        Serializer serializer = SerializerManager.getSerializer(OssConfigs.DEFAULT_SERIALIZER);
        byte[] serialized = serializer.serialize(request, DownloadRequest.class);
        FastOssCommand command = (FastOssCommand)client.getCommandFactory().createRequest(serialized, commandCode);

        Url url = Url.parseString("127.0.0.1:9999?conn=10");

        ByteBuf content = null;
        try{
            FastOssCommand response = (FastOssCommand)client.sendSync(url, command, null);
            if(response.getCommandCode().equals(FastOssProtocol.OBJECT_NOT_FOUND)){
                return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND);
            }
            content = response.getData();
            DefaultFullHttpResponse httpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, content);
//            httpResponse.headers().add("Content-Type", "image/jpeg");
            return httpResponse;
        }catch (Exception e){
            log.error("download service error: ", e);
            return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
