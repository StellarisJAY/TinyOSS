package com.jay.oss.common.remoting.builder;

import com.jay.oss.common.remoting.BuilderStrategy;
import com.jay.oss.common.remoting.TinyOssCommand;
import com.jay.oss.common.remoting.TinyOssProtocol;
import io.netty.buffer.ByteBuf;

/**
 *
 * @author Jay
 * @date 2022/04/20 16:02
 */
public class ByteBufStrategy implements BuilderStrategy {
    @Override
    public TinyOssCommand build(TinyOssCommand.TinyOssCommandBuilder builder, Object o) {
        ByteBuf data = (ByteBuf) o;
        return builder.data(data)
                .length(data.readableBytes() + TinyOssProtocol.HEADER_LENGTH)
                .build();
    }
}
