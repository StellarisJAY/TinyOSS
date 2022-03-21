package com.jay.oss.proxy.handler;

import com.jay.oss.common.util.StringUtil;
import com.jay.oss.proxy.constant.HttpConstants;
import com.jay.oss.proxy.http.OssHttpRequest;
import com.jay.oss.proxy.http.handler.AbstractHttpRequestHandler;
import com.jay.oss.proxy.service.BucketService;
import com.jay.oss.proxy.util.HttpUtil;
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

        if(StringUtil.isNullOrEmpty(bucket)){
            return HttpUtil.badRequestResponse("Missing Bucket Name");
        }
        if(HttpConstants.LIST_SERVICE.equals(bucket)){
            if(request.containsParameter(HttpConstants.PAGE) && request.containsParameter(HttpConstants.PAGE_SIZE)){
                int page = Integer.parseInt(request.getParameter(HttpConstants.PAGE));
                int pageSize = Integer.parseInt(request.getParameter(HttpConstants.PAGE_SIZE));
                return bucketService.getService(page, pageSize);
            }
            return bucketService.getService(1, 10);
        }else{
            return bucketService.listBucket(bucket, token, 10, 0);
        }

    }
}
