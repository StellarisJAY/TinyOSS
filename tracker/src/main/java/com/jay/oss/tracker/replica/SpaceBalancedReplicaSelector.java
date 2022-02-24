package com.jay.oss.tracker.replica;

import com.jay.oss.common.registry.StorageNodeInfo;

import java.util.List;

/**
 * <p>
 *  空间均衡备份选择器
 *  优先选择空间剩余最多的节点作为备份节点
 * </p>
 *
 * @author Jay
 * @date 2022/02/24 12:03
 */
public class SpaceBalancedReplicaSelector extends AbstractReplicaSelector{
    @Override
    public List<StorageNodeInfo> doSelect(List<StorageNodeInfo> filtered, int count) {
        // 按space大小降序排序
        filtered.sort((n1,n2)->(int)(n2.getSpace()-n1.getSpace()));
        return filtered.subList(0, count);
    }
}
