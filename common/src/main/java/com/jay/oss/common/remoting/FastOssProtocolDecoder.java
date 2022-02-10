package com.jay.oss.common.remoting;

import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.protocol.ProtocolDecoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
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
            读取协议HEADER
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
            // 读 content
            if(length - FastOssProtocol.HEADER_LENGTH > 0){
                // 如果该报文是上传文件分片，将数据部分ByteBuf拷贝，在后续的processor中使用零拷贝写入
                if(code == FastOssProtocol.UPLOAD_FILE_PARTS.value()){
                    /*
                        性能瓶颈：copy会复制整个in buffer，buffer大小最大可达16MB，浪费内存空间
                        使用slice来避免复制，但是需要retain来避免内存泄漏
                     */
                    in.retain();
                    ByteBuf data = in.slice(in.readerIndex(), length - FastOssProtocol.HEADER_LENGTH);
                    in.readerIndex(in.readerIndex() + length - FastOssProtocol.HEADER_LENGTH);
                    commandBuilder.data(data);
                }else{
                    byte[] content = new byte[length - FastOssProtocol.HEADER_LENGTH];
                    in.readBytes(content);
                    commandBuilder.content(content);
                }
            }
            out.add(commandBuilder.build());
        }else{
            // 有 TCP 拆包，重置readerIndex
            in.resetReaderIndex();
        }
    }

    /**
     *  拷贝一定长度的数据到新的byteBuf中，该过程使用直接内存和readBytes零拷贝完成
     * @param in {@link ByteBuf} src
     * @param length copied length
     * @return {@link ByteBuf} copied parts of the original buffer
     */
    private ByteBuf copyByteBuf(ByteBuf in, int length){
        ByteBuf buffer = Unpooled.directBuffer(length);
        in.readBytes(buffer, length);
        return buffer;
    }
}
