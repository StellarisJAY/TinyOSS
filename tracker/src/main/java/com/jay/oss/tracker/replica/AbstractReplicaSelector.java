package com.jay.oss.tracker.replica;

import com.jay.oss.common.registry.StorageNodeInfo;

import java.util.List;
import java.util.stream.Collectors;

/**
 * <p>
 *  Replica选择抽象类
 * </p>
 *
 * @author Jay
 * @date 2022/02/24 11:54
 */
public abstract class AbstractReplicaSelector implements ReplicaSelector {
    @Override
    public List<StorageNodeInfo> select(List<StorageNodeInfo> candidates, long size, int count, String mainReplica) throws Exception{
        // 候选节点不足，即集群中初始的节点数量不足
        if(candidates.size() < count){
            throw new Exception("no enough storages for " + count + " replicas");
        }
        // 过滤掉 无效节点、空间不足节点、主副本节点
        List<StorageNodeInfo> filtered = candidates.stream()
                .filter(candidate -> candidate.isAvailable() && candidate.getSpace() >= size && !mainReplica.equals(candidate.getUrl()))
                .collect(Collectors.toList());
        // 过滤后节点不足
        if(filtered.size() < count){
            throw new Exception("no enough storages for " + count + " replicas");
        }
        // 执行选择逻辑
        return doSelect(filtered, count);
    }

    /**
     * 选择逻辑
     * @param filtered 过滤后的候选节点
     * @param count 备份数量
     * @return List
     */
    public abstract List<StorageNodeInfo> doSelect(List<StorageNodeInfo> filtered, int count);
}
