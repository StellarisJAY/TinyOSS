package com.jay.oss.proxy.service;

import com.jay.dove.DoveClient;
import com.jay.dove.serialize.Serializer;
import com.jay.dove.serialize.SerializerManager;
import com.jay.dove.transport.Url;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.OssConfigs;
import com.jay.oss.common.entity.DeleteRequest;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 *
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
     * @return {@link FullHttpResponse}
     */
    public FullHttpResponse deleteObject(String key, String bucket){
        DeleteRequest requestBody = DeleteRequest.builder().key(bucket + key).build();

        Serializer serializer = SerializerManager.getSerializer(OssConfigs.DEFAULT_SERIALIZER);
        byte[] content = serializer.serialize(requestBody, DeleteRequest.class);
        FastOssCommand request = (FastOssCommand)client.getCommandFactory().createRequest(content, FastOssProtocol.DELETE_OBJECT);
        Url url = Url.parseString("127.0.0.1:9999?conn=10");

        FullHttpResponse httpResponse = null;
        try{
            FastOssCommand response = (FastOssCommand)client.sendSync(url, request, null);
            CommandCode code = response.getCommandCode();
            if(code.equals(FastOssProtocol.SUCCESS)){
                httpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
            }else if (code.equals(FastOssProtocol.OBJECT_NOT_FOUND)){
                httpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND);
            }else{
                httpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
            }
        }catch (Exception e){
            log.error("delete object error, ", e);
            httpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
        return httpResponse;
    }
}
