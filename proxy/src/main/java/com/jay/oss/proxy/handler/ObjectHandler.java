package com.jay.oss.proxy.handler;

import com.jay.oss.proxy.http.handler.AbstractHttpRequestHandler;
import com.jay.oss.proxy.service.DownloadService;
import com.jay.oss.proxy.service.ObjectService;
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
    private final ObjectService objectService;

    public ObjectHandler(UploadService uploadService, DownloadService downloadService, ObjectService objectService) {
        this.uploadService = uploadService;
        this.downloadService = downloadService;
        this.objectService = objectService;
    }

    @Override
    public FullHttpResponse handlePut(ChannelHandlerContext context, FullHttpRequest request) throws Exception {
        HttpHeaders headers = request.headers();
        String key = request.uri();
        String host = headers.get("Host");
        String auth = headers.get("Authorization");
        String bucket = host.trim().substring(0, host.indexOf("."));

        ByteBuf content = request.content();
        try{
            return uploadService.putObject(key, bucket, auth, content);
        }catch (Exception e){
            log.warn("put object error ", e);
            return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public FullHttpResponse handleGet(ChannelHandlerContext context, FullHttpRequest request) throws Exception {
        HttpHeaders headers = request.headers();
        String key = request.uri();
        String host = headers.get("Host");
        String range = headers.get("Range");
        String token = headers.get("Authorization");
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
        return downloadService.getObject(key, bucket, token,  startByte, endByte);
    }

    @Override
    public FullHttpResponse handleDelete(ChannelHandlerContext context, FullHttpRequest request) throws Exception {
        String key = request.uri();
        HttpHeaders headers = request.headers();
        String host = headers.get("Host");
        String bucket = host.trim().substring(0, host.indexOf("."));

        return objectService.deleteObject(key, bucket);

    }
}
