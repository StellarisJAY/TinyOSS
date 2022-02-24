package com.jay.oss.tracker.replica;

import com.jay.oss.common.registry.StorageNodeInfo;

import java.util.List;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/02/24 11:53
 */
public interface ReplicaSelector {

    /**
     * 选择副本节点
     * @param candidates 备选节点
     * @param size 对象大小
     * @param count 副本数量
     * @param mainReplica 主节点
     * @throws Exception 候选节点不足
     * @return List
     */
    List<StorageNodeInfo> select(List<StorageNodeInfo> candidates, long size, int count, String mainReplica) throws Exception;
}
