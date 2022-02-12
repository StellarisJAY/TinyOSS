package com.jay.oss.proxy.handler;

import com.jay.oss.proxy.http.handler.AbstractHttpRequestHandler;
import com.jay.oss.proxy.service.DownloadService;
import com.jay.oss.proxy.service.UploadService;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import io.netty.util.internal.StringUtil;
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
    private final DownloadService downloadService;
    public ObjectHandler(UploadService uploadService, DownloadService downloadService) {
        this.uploadService = uploadService;
        this.downloadService = downloadService;
    }

    @Override
    public FullHttpResponse handlePut(ChannelHandlerContext context, FullHttpRequest request) throws Exception {
        HttpHeaders headers = request.headers();
        String key = request.uri();
        String host = headers.get("Host");
        String auth = headers.get("Authorization");
        String bucket = host.trim().substring(0, host.indexOf("."));

        ByteBuf content = request.content();
        uploadService.putObject(key, bucket, auth, content);

        return new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.OK);
    }

    @Override
    public FullHttpResponse handleGet(ChannelHandlerContext context, FullHttpRequest request) throws Exception {
        HttpHeaders headers = request.headers();
        String key = request.uri();
        String host = headers.get("Host");
        String range = headers.get("Range");
        String bucket = host.trim().substring(0, host.indexOf("."));
        int startByte = 0;
        int endByte = -1;
        if(!StringUtil.isNullOrEmpty(range)){
            range = range.trim().replace(' ', '\0');
            int index;
            if((index = range.indexOf("bytes=")) != -1){
                String[] parts = range.substring(index + "bytes=".length()).split("-");
                if(parts.length == 2){
                    startByte = Integer.parseInt(parts[0]);
                    endByte = Integer.parseInt(parts[1]);
                }
            }
        }
        return downloadService.getObject(key, bucket, startByte, endByte);
    }
}
