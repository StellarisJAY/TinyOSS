package com.jay.oss.common.remoting;

import com.jay.dove.transport.protocol.ProtocolEncoder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 *  Fast-OSS TCP通信协议Encoder
 * </p>
 *
 * @author Jay
 * @date 2022/01/17 14:35
 */
@Slf4j
public class TinyOssProtocolEncoder implements ProtocolEncoder {
    @Override
    public void encode(ChannelHandlerContext channelHandlerContext, Object msg, ByteBuf out) {
        if(msg instanceof TinyOssCommand){
            TinyOssCommand command = (TinyOssCommand) msg;
            out.writeInt(command.getLength());
            out.writeInt(command.getId());
            out.writeShort(command.getCommandCode().value());
            out.writeLong(command.getTimeoutMillis());
            out.writeByte(command.getSerializer());
            out.writeByte(command.getCompressor());


            if(command.getCommandCode().equals(TinyOssProtocol.DOWNLOAD_RESPONSE)){
                // 下载返回
                out.writeBytes(command.getData());
                command.getData().release();
            }
            else{
                out.writeBytes(command.getContent());
            }
        }
    }
}
