package com.jay.oss.common.replica;

import com.jay.dove.transport.Url;

import java.util.List;


/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/03/15 15:35
 */
public interface ReplicaReadWriteStrategy {
    /**
     * 获取客户端同步写入副本数量
     * 总数 - 同步写入数量 = 异步备份数量
     * @param storages 待写入storages
     * @return 客户端同步写入副本数量
     */
    int writeCount(List<Url> storages);

    /**
     * 读取副本时最多需要读取的主机数量
     * @param storages storages列表
     * @return 最多需要读取次数
     */
    int readCount(List<Url> storages);
}
