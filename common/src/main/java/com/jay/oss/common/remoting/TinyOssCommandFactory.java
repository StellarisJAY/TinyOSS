package com.jay.oss.common.remoting;

import com.jay.dove.config.DoveConfigs;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.remoting.builder.*;
import com.jay.oss.common.util.SerializeUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.DefaultFileRegion;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
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
@Slf4j
public class TinyOssCommandFactory implements CommandFactory {
    /**
     * 报文ID生成
     */
    private final AtomicInteger requestIdProvider = new AtomicInteger(1);

    static Map<Class<?>, BuilderStrategy> strategyHolder = new HashMap<>(16);

    static {
        strategyHolder.put(null, new EmptyContentStrategy());
        strategyHolder.put(byte[].class, new ByteArrayStrategy());
        strategyHolder.put(DefaultFileRegion.class, new FileRegionStrategy());
        strategyHolder.put(String.class, new StringStrategy());
        strategyHolder.put(ByteBuf.class, new ByteBufStrategy());
    }

    @Override
    public RemotingCommand createRequest(Object o, CommandCode commandCode) {
        TinyOssCommand.TinyOssCommandBuilder builder = getCommandBuilder(requestIdProvider.getAndIncrement(), commandCode);
        if(o == null){
            return strategyHolder.get(null).build(builder, null);
        }
        if(o instanceof ByteBuf){
            return strategyHolder.get(ByteBuf.class).build(builder, o);
        }
        BuilderStrategy strategy = strategyHolder.get(o.getClass());
        return strategy == null ? null : strategy.build(builder, o);
    }

    @Override
    public <T> RemotingCommand createRequest(T t, CommandCode commandCode, Class<T> aClass) {
        if(t instanceof Serializable){
            TinyOssCommand.TinyOssCommandBuilder builder = getCommandBuilder(requestIdProvider.getAndIncrement(), commandCode);
            byte[] content = SerializeUtil.serialize(t, aClass);
            return builder.content(content).length(TinyOssProtocol.HEADER_LENGTH + content.length).build();
        }
        return null;
    }

    @Override
    public RemotingCommand createResponse(int id, Object o, CommandCode commandCode) {
        TinyOssCommand.TinyOssCommandBuilder builder = getCommandBuilder(id, commandCode);
        if(o == null){
            return strategyHolder.get(null).build(builder, null);
        }
        if(o instanceof ByteBuf){
            return strategyHolder.get(ByteBuf.class).build(builder, o);
        }
        BuilderStrategy strategy;
        return (strategy = strategyHolder.get(o.getClass())) == null ? null : strategy.build(builder, o);
    }

    @Override
    public <T> RemotingCommand createResponse(int id, T content, Class<T> clazz, CommandCode commandCode) {
        byte[] serializedContent = SerializeUtil.serialize(content, clazz);
        return createResponse(id, serializedContent, commandCode);
    }

    @Override
    public RemotingCommand createTimeoutResponse(int id, Object o) {
        TinyOssCommand.TinyOssCommandBuilder builder = getCommandBuilder(id, TinyOssProtocol.REQUEST_TIMEOUT);
        if(o instanceof String){
            byte[] content = ((String) o).getBytes(DoveConfigs.DEFAULT_CHARSET);
            return builder.content(content)
                    .length(TinyOssProtocol.HEADER_LENGTH + content.length)
                    .build();
        }else{
            return builder.length(TinyOssProtocol.HEADER_LENGTH).build();
        }
    }

    @Override
    public RemotingCommand createExceptionResponse(int id, String s) {
        byte[] content = s.getBytes(OssConfigs.DEFAULT_CHARSET);
        return TinyOssCommand.builder()
                .id(id).commandCode(TinyOssProtocol.ERROR)
                .content(content).length(TinyOssProtocol.HEADER_LENGTH + content.length)
                .timeout(System.currentTimeMillis() * 2)
                .serializer(OssConfigs.DEFAULT_SERIALIZER).build();
    }

    @Override
    public RemotingCommand createExceptionResponse(int id, Throwable throwable) {
        byte[] content = throwable.getMessage().getBytes(OssConfigs.DEFAULT_CHARSET);
        return TinyOssCommand.builder()
                .id(id).commandCode(TinyOssProtocol.ERROR)
                .content(content).length(TinyOssProtocol.HEADER_LENGTH + content.length)
                .timeout(System.currentTimeMillis() * 2)
                .serializer(OssConfigs.DEFAULT_SERIALIZER).build();
    }

    private TinyOssCommand.TinyOssCommandBuilder getCommandBuilder(int id, CommandCode code){
        return TinyOssCommand.builder()
                .id(id)
                .timeout(System.currentTimeMillis() * 2)
                .serializer(OssConfigs.DEFAULT_SERIALIZER)
                .commandCode(code);
    }
}
