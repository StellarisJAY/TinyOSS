package com.jay.oss.common.remoting;

import com.jay.dove.transport.protocol.ProtocolEncoder;
import com.jay.oss.common.fs.FilePartWrapper;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 *  Fast-OSS in-net protocol encoder
 * </p>
 *
 * @author Jay
 * @date 2022/01/17 14:35
 */
@Slf4j
public class FastOssProtocolEncoder implements ProtocolEncoder {
    @Override
    public void encode(ChannelHandlerContext channelHandlerContext, Object msg, ByteBuf out) {
        if(msg instanceof FastOssCommand){
            FastOssCommand command = (FastOssCommand) msg;
            out.writeInt(command.getLength());
            out.writeInt(command.getId());
            out.writeShort(command.getCommandCode().value());
            out.writeLong(command.getTimeoutMillis());
            out.writeByte(command.getSerializer());
            out.writeByte(command.getCompressor());

            // 文件分片上传报文需要单独解析
            if(command.getCommandCode().equals(FastOssProtocol.UPLOAD_FILE_PARTS)){
                FilePartWrapper partWrapper = command.getFilePartWrapper();
                // 写入key长度
                out.writeInt(partWrapper.getKeyLength());
                // 写入key
                out.writeBytes(partWrapper.getKey());
                // 写入分片号
                out.writeInt(partWrapper.getPartNum());
                // 写入content
                int readerIndex = partWrapper.getFullContent().readerIndex();
                out.writeBytes(partWrapper.getFullContent(), partWrapper.getIndex(), partWrapper.getLength());
                // 释放一个 content refCnt
                partWrapper.getFullContent().release();
            } else{
                out.writeBytes(command.getContent());
            }
        }
    }
}
