package com.jay.oss.common.remoting;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/04/20 15:55
 */
public interface BuilderStrategy {
    TinyOssCommand build(TinyOssCommand.TinyOssCommandBuilder builder, Object o);
}
