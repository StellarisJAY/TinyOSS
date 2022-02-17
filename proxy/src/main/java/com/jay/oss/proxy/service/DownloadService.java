package com.jay.oss.proxy.service;

import com.jay.dove.DoveClient;
import com.jay.dove.transport.Url;
import com.jay.dove.transport.command.CommandCode;
import com.jay.oss.common.acl.BucketAccessMode;
import com.jay.oss.common.entity.CheckBucketAclRequest;
import com.jay.oss.common.entity.DownloadRequest;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.util.SerializeUtil;
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
     * @param token AccessToken
     * @param rangeStart 起始字节
     * @param rangeEnd 结束字节
     * @return {@link FullHttpResponse}
     */
    public FullHttpResponse getObject(String key, String bucket, String token, int rangeStart, int rangeEnd){
        // 根据范围判断下载类型，full或者ranged
        CommandCode commandCode = rangeEnd == -1 ? FastOssProtocol.DOWNLOAD_FULL : FastOssProtocol.DOWNLOAD_RANGED;
        // 创建下载请求
        DownloadRequest request = new DownloadRequest(bucket + key, rangeEnd == -1, rangeStart, rangeEnd);
        // 序列化
        byte[] serialized = SerializeUtil.serialize(request, DownloadRequest.class);
        // 创建command
        FastOssCommand command = (FastOssCommand)client.getCommandFactory().
                createRequest(serialized, commandCode);

        // 选择storage
        Url url = Url.parseString("127.0.0.1:9999?conn=10");

        ByteBuf content;
        try{
            // 检查存储桶
            CommandCode checkBucket = checkBucket(bucket, token, BucketAccessMode.READ);
            if(checkBucket.equals(FastOssProtocol.SUCCESS)){
                // 发送下载请求
                FastOssCommand response = (FastOssCommand)client.sendSync(url, command, null);
                // object不存在
                if(response.getCommandCode().equals(FastOssProtocol.OBJECT_NOT_FOUND)){
                    return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND);
                }
                content = response.getData();
                return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, content);
            }
            else if(checkBucket.equals(FastOssProtocol.ACCESS_DENIED)){
                // 无访问权限
                return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.UNAUTHORIZED);
            }
            else{
                // 桶不存在
                return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND);
            }

        }catch (Exception e){
            log.error("download service error: ", e);
            return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }


    /**
     * 检查桶访问权限
     * @param bucket 桶
     * @param token AccessToken
     * @param accessMode {@link BucketAccessMode}
     * @return boolean
     * @throws Exception e
     */
    public CommandCode checkBucket(String bucket, String token, BucketAccessMode accessMode) throws Exception{
        // search for bucket acl and check auth
        Url url = Url.parseString("127.0.0.1:9999");
        CheckBucketAclRequest request = CheckBucketAclRequest.builder()
                .accessMode(accessMode).token(token).bucket(bucket).build();
        byte[] content = SerializeUtil.serialize(request, CheckBucketAclRequest.class);
        // 创建check acl请求
        FastOssCommand command = (FastOssCommand) client.getCommandFactory()
                .createRequest(content, FastOssProtocol.CHECK_BUCKET_ACL);
        // 同步发送
        FastOssCommand response = (FastOssCommand)client.sendSync(url, command, null);
        return response.getCommandCode();
    }
}
