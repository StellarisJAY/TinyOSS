package com.jay.oss.common.remoting.builder;

import com.jay.oss.common.remoting.BuilderStrategy;
import com.jay.oss.common.remoting.TinyOssCommand;
import com.jay.oss.common.remoting.TinyOssProtocol;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/04/20 16:03
 */
public class ByteArrayStrategy implements BuilderStrategy {
    @Override
    public TinyOssCommand build(TinyOssCommand.TinyOssCommandBuilder builder, Object o) {
        byte[] content = (byte[])o;
        return builder.content(content)
                .length(TinyOssProtocol.HEADER_LENGTH + content.length)
                .build();
    }
}
