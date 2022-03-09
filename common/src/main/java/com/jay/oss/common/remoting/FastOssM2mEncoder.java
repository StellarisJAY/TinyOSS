package com.jay.oss.common.remoting;

import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.protocol.ProtocolM2mEncoder;
import com.jay.oss.common.fs.FilePartWrapper;
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
public class FastOssM2mEncoder implements ProtocolM2mEncoder {
    @Override
    public void encode(ChannelHandlerContext context, Object o, List<Object> out) {
        if(o instanceof FastOssCommand){
            FastOssCommand command = (FastOssCommand) o;
            // 创建header
            ByteBuf header = createHeader(command);
            // 输出header
            out.add(header);
            CommandCode code = command.getCommandCode();
            // 处理文件分片
            if(FastOssProtocol.UPLOAD_FILE_PARTS.equals(code)){
                // 创建文件分片directBuffer
                ByteBuf filePart = createFilePartContent(command.getFilePartWrapper());
                out.add(filePart);
            }
            else if(FastOssProtocol.MULTIPART_UPLOAD_PART.equals(code)){
                ByteBuf multipartContent = createMultipartContent(command.getFilePartWrapper());
                out.add(multipartContent);
            }
            // 处理下载的fileRegion
            else if(FastOssProtocol.DOWNLOAD_RESPONSE.equals(code)){
                out.add(command.getFileRegion());
            }
            // 处理序列化的content
            else if(command.getContent() != null){
                out.add(Unpooled.wrappedBuffer(command.getContent()));
            }
        }
    }

    /**
     * 创建Header
     * @param command {@link FastOssCommand}
     * @return {@link ByteBuf}
     */
    private ByteBuf createHeader(FastOssCommand command){
        ByteBuf header = Unpooled.directBuffer(FastOssProtocol.HEADER_LENGTH);
        header.writeByte(FastOssProtocol.PROTOCOL_CODE.value());
        header.writeInt(command.getLength());
        header.writeInt(command.getId());
        header.writeShort(command.getCommandCode().value());
        header.writeLong(command.getTimeoutMillis());
        header.writeByte(command.getSerializer());
        header.writeByte(command.getCompressor());
        return header;
    }

    private ByteBuf createFilePartContent(FilePartWrapper partWrapper){
        ByteBuf out = Unpooled.directBuffer(partWrapper.getKeyLength() + 8 + partWrapper.getLength());
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
        return out;
    }

    private ByteBuf createMultipartContent(FilePartWrapper partWrapper){
        ByteBuf out = Unpooled.directBuffer(partWrapper.getKeyLength() + 8 + partWrapper.getLength());
        out.writeInt(partWrapper.getKeyLength());
        out.writeBytes(partWrapper.getKey());
        out.writeInt(partWrapper.getPartNum());
        out.writeBytes(partWrapper.getFullContent());
        partWrapper.getFullContent().release();
        return out;
    }
}
