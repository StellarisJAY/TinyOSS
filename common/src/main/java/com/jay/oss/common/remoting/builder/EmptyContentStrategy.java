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
 * @date 2022/04/20 16:04
 */
public class EmptyContentStrategy implements BuilderStrategy {
    @Override
    public TinyOssCommand build(TinyOssCommand.TinyOssCommandBuilder builder, Object o) {
        return builder.length(TinyOssProtocol.HEADER_LENGTH).build();
    }
}
