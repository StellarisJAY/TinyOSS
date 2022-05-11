package com.jay.oss.tracker.registry;

import com.jay.oss.common.registry.Registry;
import com.jay.oss.common.registry.StorageNodeInfo;
import com.jay.oss.tracker.replica.ReplicaSelector;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

/**
 * <p>
 *  节点注册中心
 * </p>
 *
 * @author Jay
 * @date 2022/05/09 10:01
 */
@Slf4j
public class StorageNodeRegistry {
    private final Registry registry;
    private final ReplicaSelector replicaSelector;

    public StorageNodeRegistry(Registry registry, ReplicaSelector replicaSelector) {
        this.registry = registry;
        this.replicaSelector = replicaSelector;
    }

    public void init() throws Exception{
        Map<String, StorageNodeInfo> storageNodes = registry.lookupAll();
        log.info("Storage Node registry initialized, Registered Storage nodes: {}", storageNodes.keySet());
    }

    /**
     * 根据选择策略选择上传对象的storage节点
     * @param objectKey 对象key
     * @param size 对象大小
     * @param replica 副本数量
     * @return {@link List<StorageNodeInfo>} storage节点列表
     * @throws Exception e
     */
    public List<StorageNodeInfo> selectUploadNode(String objectKey, long size, int replica) throws Exception {
        List<StorageNodeInfo> aliveNodes = registry.aliveNodes();
        return replicaSelector.select(aliveNodes, size, replica);
    }
}
