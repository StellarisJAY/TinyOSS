package com.jay.oss.common.remoting;

import com.jay.dove.transport.protocol.ProtocolEncoder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * <p>
 *  Fast-OSS inet protocol encoder
 * </p>
 *
 * @author Jay
 * @date 2022/01/17 14:35
 */
public class FastOssProtocolEncoder implements ProtocolEncoder {
    @Override
    public void encode(ChannelHandlerContext channelHandlerContext, Object msg, ByteBuf out) {
        if(msg instanceof FastOssCommand){
            FastOssCommand command = (FastOssCommand) msg;
            out.writeInt(command.getLength());
            out.writeInt(command.getId());
            out.writeShort(command.getCommandCode().value());
            out.writeByte(command.getSerializer().getSerializerCode());
            out.writeByte(command.getCompressor());
            out.writeBytes(command.getContent());
        }
    }
}
