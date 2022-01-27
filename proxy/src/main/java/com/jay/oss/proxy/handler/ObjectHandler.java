package com.jay.oss.proxy.handler;

import com.jay.oss.proxy.http.handler.AbstractHttpRequestHandler;
import com.jay.oss.proxy.service.UploadService;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 *  对象请求处理器
 * </p>
 *
 * @author Jay
 * @date 2022/01/25 20:10
 */
@Slf4j
public class ObjectHandler extends AbstractHttpRequestHandler {

    private final UploadService uploadService;

    public ObjectHandler(UploadService uploadService) {
        this.uploadService = uploadService;
    }

    @Override
    public FullHttpResponse handlePut(ChannelHandlerContext context, FullHttpRequest request) throws Exception {
        HttpHeaders headers = request.headers();
        String key = headers.get("Key");
        String host = headers.get("Host");
        String auth = headers.get("Authorization");
        String bucket = host.trim().substring(0, host.indexOf("."));

        ByteBuf content = request.content();
        uploadService.putObject(key, bucket, auth, content);

        return new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.OK);
    }
}
