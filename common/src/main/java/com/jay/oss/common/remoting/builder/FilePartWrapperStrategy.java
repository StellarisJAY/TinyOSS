package com.jay.oss.common.remoting.builder;

import com.jay.oss.common.fs.FilePartWrapper;
import com.jay.oss.common.remoting.BuilderStrategy;
import com.jay.oss.common.remoting.TinyOssCommand;
import com.jay.oss.common.remoting.TinyOssProtocol;

/**
 * @author Jay
 * @date 2022/04/20 16:06
 */
public class FilePartWrapperStrategy implements BuilderStrategy {
    @Override
    public TinyOssCommand build(TinyOssCommand.TinyOssCommandBuilder builder, Object o) {
        FilePartWrapper partWrapper = (FilePartWrapper) o;
        return builder.filePartWrapper(partWrapper)
                .length(TinyOssProtocol.HEADER_LENGTH + partWrapper.getKeyLength() + partWrapper.getLength() + 8)
                .build();
    }
}
