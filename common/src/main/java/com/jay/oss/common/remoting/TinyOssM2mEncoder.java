package com.jay.oss.common.remoting;

import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.protocol.ProtocolM2mEncoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * <p>
 *  FastOss MessageToMessage Encoder
 * </p>
 *
 * @author Jay
 * @date 2022/02/13 12:59
 */
public class TinyOssM2mEncoder implements ProtocolM2mEncoder {
    @Override
    public void encode(ChannelHandlerContext context, Object o, List<Object> out) {
        if(o instanceof TinyOssCommand){
            TinyOssCommand command = (TinyOssCommand) o;
            // 创建header
            ByteBuf header = createHeader(command);
            // 输出header
            out.add(header);
            CommandCode code = command.getCommandCode();
            // 处理文件分片
            if(TinyOssProtocol.UPLOAD_REQUEST.equals(code)){
                out.add(command.getData());
            }
            // 处理下载的fileRegion
            else if(TinyOssProtocol.DOWNLOAD_RESPONSE.equals(code)){
                out.add(command.getData());
            }
            // 处理序列化的content
            else if(command.getContent() != null){
                out.add(Unpooled.wrappedBuffer(command.getContent()));
            }
        }
    }

    /**
     * 创建Header
     * @param command {@link TinyOssCommand}
     * @return {@link ByteBuf}
     */
    private ByteBuf createHeader(TinyOssCommand command){
        ByteBuf header = Unpooled.directBuffer(TinyOssProtocol.HEADER_LENGTH);
        header.writeByte(TinyOssProtocol.PROTOCOL_CODE.value());
        header.writeInt(command.getLength());
        header.writeInt(command.getId());
        header.writeShort(command.getCommandCode().value());
        header.writeLong(command.getTimeoutMillis());
        header.writeByte(command.getSerializer());
        header.writeByte(command.getCompressor());
        return header;
    }
}
