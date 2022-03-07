package com.jay.oss.proxy.handler;

import com.jay.oss.proxy.constant.HttpConstants;
import com.jay.oss.proxy.http.handler.AbstractHttpRequestHandler;
import com.jay.oss.proxy.service.DownloadService;
import com.jay.oss.proxy.service.MultipartUploadService;
import com.jay.oss.proxy.service.ObjectService;
import com.jay.oss.proxy.service.UploadService;
import com.jay.oss.proxy.util.HttpUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import io.netty.util.internal.StringUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

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
    private final MultipartUploadService multipartUploadService;

    public ObjectHandler(UploadService uploadService, DownloadService downloadService, ObjectService objectService, MultipartUploadService multipartUploadService) {
        this.uploadService = uploadService;
        this.downloadService = downloadService;
        this.objectService = objectService;
        this.multipartUploadService = multipartUploadService;
    }

    @Override
    public FullHttpResponse handlePut(ChannelHandlerContext context, FullHttpRequest request) throws Exception {
        HttpHeaders headers = request.headers();
        String uri = request.uri();
        int idx = uri.indexOf("?");
        String key;
        if(idx == -1){
            key = uri.substring(1);
        }else{
            key = uri.substring(1, idx);
        }
        String host = headers.get("Host");
        String auth = headers.get("Authorization");
        String bucket = host.trim().substring(0, host.indexOf("."));
        Map<String, String> parameters = HttpUtil.parseUri(uri);

        ByteBuf content = request.content();
        try{
            if(parameters.containsKey(HttpConstants.UPLOAD_ID) && parameters.containsKey(HttpConstants.UPLOAD_PART_NUM)){
                  return super.handlePut(context, request);
            }else{
                return uploadService.putObject(key, bucket, auth, content);
            }
        }catch (Exception e){
            log.warn("put object error ", e);
            return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public FullHttpResponse handleGet(ChannelHandlerContext context, FullHttpRequest request) throws Exception {
        HttpHeaders headers = request.headers();
        String uri = request.uri();
        int idx = uri.indexOf("?");
        String key;
        if(idx == -1){
            key = uri.substring(1);
        }else{
            key = uri.substring(1, idx);
        }
        String host = headers.get("Host");
        String range = headers.get("Range");
        String token = headers.get("Authorization");
        String bucket = host.trim().substring(0, host.indexOf("."));
        Map<String, String> parameters = HttpUtil.parseUri(uri);

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
        return downloadService.getObject(key, bucket, token, parameters.get("versionId"),  startByte, endByte);
    }

    @Override
    public FullHttpResponse handleDelete(ChannelHandlerContext context, FullHttpRequest request) throws Exception {
        String key = request.uri();
        HttpHeaders headers = request.headers();
        String host = headers.get("Host");
        String bucket = host.trim().substring(0, host.indexOf("."));
        String token = headers.get("Authorization");
        return objectService.deleteObject(key, bucket, token);
    }

    @Override
    public FullHttpResponse handlePost(ChannelHandlerContext context, FullHttpRequest request) throws Exception {
        String uri = request.uri();
        HttpHeaders headers = request.headers();
        String host = headers.get("Host");
        String token = headers.get("Authorization");
        String bucket = host.trim().substring(0, host.indexOf("."));
        String key = uri.substring(1, uri.indexOf("?"));
        int length = Integer.parseInt(headers.get("Content-Length"));
        Map<String, String> parameters = HttpUtil.parseUri(uri);

        if(parameters.containsKey(HttpConstants.INIT_UPLOAD_PARAMETER)){
            return multipartUploadService.initializeMultipartUpload(key, bucket, token, length);
        }

        return HttpUtil.badRequestResponse();
    }
}
