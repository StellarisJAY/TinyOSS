package com.jay.oss.proxy.handler;

import com.jay.oss.proxy.constant.HttpConstants;
import com.jay.oss.proxy.http.OssHttpRequest;
import com.jay.oss.proxy.http.handler.AbstractHttpRequestHandler;
import com.jay.oss.proxy.service.DownloadService;
import com.jay.oss.proxy.service.MultipartUploadService;
import com.jay.oss.proxy.service.ObjectService;
import com.jay.oss.proxy.service.UploadService;
import com.jay.oss.proxy.util.HttpUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
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
    private final MultipartUploadService multipartUploadService;

    public ObjectHandler(UploadService uploadService, DownloadService downloadService, ObjectService objectService, MultipartUploadService multipartUploadService) {
        this.uploadService = uploadService;
        this.downloadService = downloadService;
        this.objectService = objectService;
        this.multipartUploadService = multipartUploadService;
    }

    @Override
    public FullHttpResponse handlePut(ChannelHandlerContext context, OssHttpRequest request)  {

        String host = request.host();
        String auth = request.authorization();
        String bucket = host.trim().substring(0, host.indexOf("."));
        String key = request.getPath();
        String md5 = request.contentMd5() == null ? "" : request.contentMd5();
        ByteBuf content = request.content();
        try{
            if(request.containsParameter(HttpConstants.UPLOAD_ID) && request.containsParameter(HttpConstants.UPLOAD_PART_NUM)){
                return multipartUploadService.putObject(key, bucket, auth, request.getParameter(HttpConstants.UPLOAD_ID),
                        request.getParameter("versionId"), Integer.parseInt(request.getParameter(HttpConstants.UPLOAD_PART_NUM)), content);
            }else{
                return uploadService.putObject(key, bucket, auth, md5, content);
            }
        }catch (Exception e){
            log.warn("put object error ", e);
            return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public FullHttpResponse handleGet(ChannelHandlerContext context, OssHttpRequest request)  {
        String key = request.getPath();
        String host = request.host();
        String token = request.authorization();
        String bucket = host.trim().substring(0, host.indexOf("."));
        String range = request.range();
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
        return downloadService.getObject(key, bucket, token, request.getParameter("versionId"),  startByte, endByte);
    }

    @Override
    public FullHttpResponse handleDelete(ChannelHandlerContext context, OssHttpRequest request)  {
        String key = request.getPath();
        String host = request.host();
        String token = request.authorization();
        String bucket = host.trim().substring(0, host.indexOf("."));
        return objectService.deleteObject(key, bucket,request.getParameter(HttpConstants.VERSION_ID), token);
    }

    @Override
    public FullHttpResponse handlePost(ChannelHandlerContext context, OssHttpRequest request) {
        String key = request.getPath();
        String host = request.host();
        String token = request.authorization();
        String bucket = host.trim().substring(0, host.indexOf("."));
        int length = request.contentLength();
        if(request.containsParameter(HttpConstants.INIT_UPLOAD_PARAMETER)){

            return multipartUploadService.initializeMultipartUpload(key, bucket, token, length);
        }
        else if(request.containsParameter(HttpConstants.UPLOAD_ID)){
            String md5 = request.contentMd5();
            String uploadId = request.getParameter(HttpConstants.UPLOAD_ID);
            String versionId = request.getParameter(HttpConstants.VERSION_ID);
            int parts = Integer.parseInt(request.getParameter("parts"));
            return multipartUploadService.completeMultipartUpload(key, bucket, versionId == null ? "" : versionId, token, uploadId, parts, md5, length);
        }
        return HttpUtil.badRequestResponse();
    }

    @Override
    public FullHttpResponse handleHead(ChannelHandlerContext context, OssHttpRequest request) throws Exception {
        String key = request.getPath();
        String host = request.host();
        String token = request.authorization();
        String bucket = host.trim().substring(0, host.indexOf("."));
        return objectService.getObjectMeta(key, bucket, request.getParameter(HttpConstants.VERSION_ID), token);
    }
}
