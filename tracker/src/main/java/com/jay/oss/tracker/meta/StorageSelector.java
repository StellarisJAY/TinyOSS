package com.jay.oss.tracker.meta;

import com.jay.oss.common.registry.StorageNodeInfo;

import java.util.List;

/**
 * <p>
 *  存储节点选择器
 * </p>
 *
 * @author Jay
 * @date 2022/02/21 11:42
 */
public interface StorageSelector {

    /**
     * 选择存储节点
     * @param nodes 存储节点
     * @param size 目标大小
     * @param replica 备份数量
     * @return 存储节点集合
     */
    List<StorageNodeInfo> select(List<StorageNodeInfo> nodes, long size, int replica);
}
