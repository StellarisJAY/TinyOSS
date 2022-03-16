package com.jay.oss.common.replica;

import com.jay.dove.transport.Url;

import java.util.List;

/**
 * <p>
 *  WARO读写策略
 *  写入时要求所有副本都写入成功才算成功，读取时只需要读一次就能读取到数据
 *  该策略牺牲了写入的速度，但是提升了读取的速度
 * </p>
 *
 * @author Jay
 * @date 2022/03/15 15:39
 */
public class WriteAllReadOneStrategy implements ReplicaReadWriteStrategy {
    @Override
    public int writeCount(List<Url> storages) {
        return storages.size();
    }

    @Override
    public int readCount(List<Url> storages) {
        return 1;
    }
}
