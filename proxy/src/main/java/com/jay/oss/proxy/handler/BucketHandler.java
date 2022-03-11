package com.jay.oss.proxy.handler;

import com.jay.oss.common.util.StringUtil;
import com.jay.oss.proxy.http.OssHttpRequest;
import com.jay.oss.proxy.http.handler.AbstractHttpRequestHandler;
import com.jay.oss.proxy.service.BucketService;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpResponse;

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
    public FullHttpResponse handlePut(ChannelHandlerContext context, OssHttpRequest request)  {
        String host = request.host();
        String bucket = host.trim().substring(0, host.indexOf("."));
        String acl = request.acl();

        if(StringUtil.isNullOrEmpty(acl)){
            acl = "private";
        }
        return bucketService.putBucket(bucket, acl, request.containsParameter("versioning"));
    }

    @Override
    public FullHttpResponse handleGet(ChannelHandlerContext context, OssHttpRequest request) {
        String host = request.host();
        String token = request.authorization();
        String bucket = host.trim().substring(0, host.indexOf("."));

        return bucketService.listBucket(bucket, token, 10, 0);
    }
}
