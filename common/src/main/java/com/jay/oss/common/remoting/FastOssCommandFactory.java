package com.jay.oss.common.remoting;

import com.jay.dove.config.Configs;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.OssConfigs;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/01/25 10:47
 */
public class FastOssCommandFactory implements CommandFactory {

    private final AtomicInteger requestIdProvider = new AtomicInteger(1);

    @Override
    public RemotingCommand createRequest(Object o, CommandCode commandCode) {
        if(o instanceof byte[]){
            byte[] content = (byte[])o;
            return FastOssCommand.builder()
                    .content(content)
                    .id(requestIdProvider.getAndIncrement())
                    .commandCode(commandCode)
                    .length(FastOssProtocol.HEADER_LENGTH + content.length)
                    .serializer(OssConfigs.DEFAULT_SERIALIZER)
                    .timeout(System.currentTimeMillis() + 10 * 1000)
                    .build();
        }
        return null;
    }

    @Override
    public RemotingCommand createResponse(int id, Object o, CommandCode commandCode) {
        return null;
    }

    @Override
    public RemotingCommand createTimeoutResponse(int id, Object o) {
        FastOssCommand.FastOssCommandBuilder builder = FastOssCommand.builder()
                .id(id)
                .timeout(-1)
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
