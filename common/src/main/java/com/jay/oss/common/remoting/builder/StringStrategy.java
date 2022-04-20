package com.jay.oss.common.remoting.builder;

import com.jay.oss.common.remoting.BuilderStrategy;
import com.jay.oss.common.remoting.TinyOssCommand;
import com.jay.oss.common.remoting.TinyOssProtocol;
import com.jay.oss.common.util.StringUtil;

/**
 *
 * @author Jay
 * @date 2022/04/20 16:02
 */
public class StringStrategy implements BuilderStrategy {
    @Override
    public TinyOssCommand build(TinyOssCommand.TinyOssCommandBuilder builder, Object o) {
        byte[] content = StringUtil.getBytes((String)o);
        return builder.content(content)
                .length(TinyOssProtocol.HEADER_LENGTH + content.length)
                .build();
    }
}
