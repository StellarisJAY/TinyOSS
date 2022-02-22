package com.jay.oss.tracker.track;

import com.jay.oss.common.registry.StorageNodeInfo;

import java.util.List;
import java.util.stream.Collectors;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/02/21 11:43
 */
public abstract class AbstractStorageSelector implements StorageSelector{
    @Override
    public List<StorageNodeInfo> select(List<StorageNodeInfo> nodes, long size, int replica) {
        // 按大小升序排序，优先选择剩余空间较多的storage
        List<StorageNodeInfo> selections = nodes.stream().sorted((o1, o2) -> (int) (o1.getSpace() - o2.getSpace()))
                .filter((nodeInfo) -> nodeInfo.isAvailable() && nodeInfo.getSpace() > size)
                .collect(Collectors.toList());
        // 节点数量是否足够备份
        if(selections.size() <= replica){
            return selections;
        }
        // 执行选择策略
        return doSelect(selections, size, replica);
    }

    /**
     * 选择策略
     * @param nodes 存储节点
     * @param size 目标大小
     * @return 存储节点
     */
    public abstract List<StorageNodeInfo> doSelect(List<StorageNodeInfo> nodes, long size, int replica);
}
