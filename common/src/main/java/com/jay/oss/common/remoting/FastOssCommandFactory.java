package com.jay.oss.common.remoting;

import com.jay.dove.config.Configs;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.OssConfigs;
import com.jay.oss.common.fs.FilePartWrapper;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 *  FastOSS命令工厂
 *  负责创建OSS命令报文
 * </p>
 *
 * @author Jay
 * @date 2022/01/25 10:47
 */
public class FastOssCommandFactory implements CommandFactory {
    /**
     * 报文ID生成
     */
    private final AtomicInteger requestIdProvider = new AtomicInteger(1);

    @Override
    public RemotingCommand createRequest(Object o, CommandCode commandCode) {
        FastOssCommand.FastOssCommandBuilder builder = FastOssCommand.builder()
                .id(requestIdProvider.getAndIncrement())
                .commandCode(commandCode)
                .serializer(OssConfigs.DEFAULT_SERIALIZER)
                .timeout(System.currentTimeMillis() + 60 * 1000);
        if(o instanceof byte[]){
            byte[] content = (byte[])o;
            return builder
                    .length(FastOssProtocol.HEADER_LENGTH + content.length)
                    .content(content).build();
        }
        else if(o instanceof ByteBuf){
            ByteBuf data = (ByteBuf) o;
            return builder.data(data)
                    .length(FastOssProtocol.HEADER_LENGTH + data.readableBytes())
                    .build();
        }
        else if (o instanceof FilePartWrapper){
            FilePartWrapper partWrapper = (FilePartWrapper) o;
            return builder.filePartWrapper(partWrapper)
                    .length(FastOssProtocol.HEADER_LENGTH + partWrapper.getKeyLength() + partWrapper.getLength() + 8)
                    .build();
        }
        return null;
    }

    @Override
    public RemotingCommand createResponse(int id, Object o, CommandCode commandCode) {
        FastOssCommand.FastOssCommandBuilder builder = FastOssCommand.builder()
                .id(id)
                .timeout(System.currentTimeMillis() * 2)
                .serializer(OssConfigs.DEFAULT_SERIALIZER)
                .compressor((byte) 0)
                .commandCode(commandCode);
        if(o instanceof String){
            byte[] content = ((String)o).getBytes(Configs.DEFAULT_CHARSET);
            return builder.content(content)
                    .length(FastOssProtocol.HEADER_LENGTH + content.length)
                    .build();
        }
        else if(o instanceof byte[]){
            byte[] content = (byte[])o;
            return builder.content(content)
                    .length(FastOssProtocol.HEADER_LENGTH + content.length)
                    .build();
        }
        else if(o instanceof ByteBuf){
            ByteBuf data = (ByteBuf) o;
            return builder.data(data)
                    .length(data.readableBytes() + FastOssProtocol.HEADER_LENGTH)
                    .build();
        }
        else{
            return builder.length(FastOssProtocol.HEADER_LENGTH).build();
        }
    }

    @Override
    public RemotingCommand createTimeoutResponse(int id, Object o) {
        FastOssCommand.FastOssCommandBuilder builder = FastOssCommand.builder()
                .id(id)
                .timeout(System.currentTimeMillis() * 2)
                .serializer(OssConfigs.DEFAULT_SERIALIZER)
                .commandCode(FastOssProtocol.REQUEST_TIMEOUT);
        if(o instanceof String){
            byte[] content = ((String) o).getBytes(Configs.DEFAULT_CHARSET);
            return builder.content(content)
                    .length(FastOssProtocol.HEADER_LENGTH + content.length)
                    .build();
        }else if (o instanceof byte[]){
            byte[] content = (byte[]) o;
            return builder.content(content)
                    .length(FastOssProtocol.HEADER_LENGTH + content.length)
                    .build();
        }else{
            return builder.length(FastOssProtocol.HEADER_LENGTH).build();
        }
    }

    @Override
    public RemotingCommand createExceptionResponse(int id, String s) {
        return null;
    }

    @Override
    public RemotingCommand createExceptionResponse(int id, Throwable throwable) {
        return null;
    }
}
