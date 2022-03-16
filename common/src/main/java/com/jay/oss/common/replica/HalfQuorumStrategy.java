package com.jay.oss.common.replica;

import com.jay.dove.transport.Url;

import java.util.List;

/**
 * <p>
 *  写半数以上Quorum策略
 *  W + R > N
 *  我们规定W必须是半数以上的
 *  那么读取最多只需要读一半的服务器
 * </p>
 *
 * @author Jay
 * @date 2022/03/15 15:42
 */
public class HalfQuorumStrategy implements ReplicaReadWriteStrategy{
    @Override
    public int writeCount(List<Url> storages) {
        return storages.size() / 2 + 1;
    }

    @Override
    public int readCount(List<Url> storages) {
        int size = storages.size();
        return (size & 1) == 1 ? size / 2 + 1 : size / 2;
    }
}
