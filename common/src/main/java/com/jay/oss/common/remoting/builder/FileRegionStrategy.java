package com.jay.oss.common.remoting.builder;

import com.jay.oss.common.remoting.BuilderStrategy;
import com.jay.oss.common.remoting.TinyOssCommand;
import com.jay.oss.common.remoting.TinyOssProtocol;
import io.netty.channel.DefaultFileRegion;

/**
 *
 * @author Jay
 * @date 2022/04/20 16:03
 */
public class FileRegionStrategy implements BuilderStrategy {
    @Override
    public TinyOssCommand build(TinyOssCommand.TinyOssCommandBuilder builder, Object o) {
        DefaultFileRegion fileRegion = (DefaultFileRegion) o;
        return builder.length(TinyOssProtocol.HEADER_LENGTH + (int) fileRegion.count())
                .fileRegion(fileRegion)
                .build();
    }
}
