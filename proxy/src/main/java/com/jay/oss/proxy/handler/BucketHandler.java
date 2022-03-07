package com.jay.oss.proxy.handler;

import com.jay.oss.common.util.StringUtil;
import com.jay.oss.proxy.http.handler.AbstractHttpRequestHandler;
import com.jay.oss.proxy.service.BucketService;
import com.jay.oss.proxy.util.HttpUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;

import java.util.Map;

/**
 * <p>
 *  桶请求处理器
 * </p>
 *
 * @author Jay
 * @date 2022/02/11 11:30
 */
public class BucketHandler extends AbstractHttpRequestHandler {

    /**
     * bucket服务
     */
    private final BucketService bucketService;

    public BucketHandler(BucketService bucketService) {
        this.bucketService = bucketService;
    }

    @Override
    public FullHttpResponse handlePut(ChannelHandlerContext context, FullHttpRequest request) throws Exception {
        HttpHeaders headers = request.headers();
        String host = headers.get("Host");
        String bucket = host.trim().substring(0, host.indexOf("."));
        String acl = headers.get("foss-acl");
        String uri = request.uri();
        Map<String, String> parameters = HttpUtil.parseUri(uri);

        if(StringUtil.isNullOrEmpty(acl)){
            acl = "private";
        }
        return bucketService.putBucket(bucket, acl, parameters.containsKey("versioning"));
    }

    @Override
    public FullHttpResponse handleGet(ChannelHandlerContext context, FullHttpRequest request) throws Exception {
        HttpHeaders headers = request.headers();
        String host = headers.get("Host");
        String bucket = host.trim().substring(0, host.indexOf("."));
        String token = headers.get("Authorization");

        return bucketService.listBucket(bucket, token, 10, 0);
    }
}
