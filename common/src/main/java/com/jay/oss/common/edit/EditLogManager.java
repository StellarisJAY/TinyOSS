package com.jay.oss.common.edit;

/**
 * <p>
 *  EditLog管理器接口
 * </p>
 *
 * @author Jay
 * @date 2022/03/01 14:25
 */
public interface EditLogManager {
    /**
     * 初始化
     */
    void init();

    /**
     * 追加editLog
     * @param editLog {@link EditLog}
     */
    void append(EditLog editLog);

    /**
     * 刷盘
     * @param force 是否强制刷盘
     */
    void flush(boolean force);

    /**
     * 加载并压缩
     * @param manager 元数据Manager
     */
    void loadAndCompress(Object manager);
}
