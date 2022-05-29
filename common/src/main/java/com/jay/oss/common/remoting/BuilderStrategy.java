package com.jay.oss.common.remoting;

/**
 * <p>
 *  报文Builder策略
 * </p>
 *
 * @author Jay
 * @date 2022/04/20 15:55
 */
public interface BuilderStrategy {
    /**
     * 根据参数类型创建报文
     * @param builder 报文Builder
     * @param o 报文参数
     * @return {@link TinyOssCommand}
     */
    TinyOssCommand build(TinyOssCommand.TinyOssCommandBuilder builder, Object o);
}
