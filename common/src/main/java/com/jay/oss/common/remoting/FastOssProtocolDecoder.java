package com.jay.oss.common.remoting;

import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.protocol.ProtocolDecoder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * <p>
 *  Fast-OSS inet protocol decoder
 * </p>
 *
 * @author Jay
 * @date 2022/01/17 14:35
 */
public class FastOssProtocolDecoder implements ProtocolDecoder {
    @Override
    public void decode(ChannelHandlerContext channelHandlerContext, ByteBuf in, List<Object> out) {
        in.markReaderIndex();
        if(in.readableBytes() < FastOssProtocol.HEADER_LENGTH){
            return;
        }
        byte proto = in.readByte();
        if(proto != FastOssProtocol.PROTOCOL_CODE.value()){
            throw new RuntimeException("Invalid protocol for FastOssProtocolDecoder, code: " + proto);
        }

        /*
            Read Headers
         */
        int length = in.readInt();
        int id = in.readInt();
        short code = in.readShort();
        long timeout = in.readLong();
        byte serializer = in.readByte();
        byte compressor = in.readByte();

        // build command
        FastOssCommand.FastOssCommandBuilder commandBuilder = FastOssCommand.builder().length(length)
                .id(id)
                .commandCode(new CommandCode(code))
                .timeout(timeout)
                .serializer(serializer)
                .compressor(compressor);

        // 检查buffer中内容是否完整，避免TCP拆包
        if(in.readableBytes() >= length - FastOssProtocol.HEADER_LENGTH){
            // 读 content，此处的逻辑有待改进，对于文件数据可以用零拷贝提高性能
            if(length - FastOssProtocol.HEADER_LENGTH > 0){
                byte[] content = new byte[length - FastOssProtocol.HEADER_LENGTH];
                in.readBytes(content);
                commandBuilder.content(content);
            }
            out.add(commandBuilder.build());
        }else{
            // 有 TCP 拆包，重置readerIndex
            in.resetReaderIndex();
        }

    }
}
