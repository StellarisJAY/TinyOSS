package com.jay.oss.tracker.registry;

import com.jay.oss.common.registry.Registry;
import com.jay.oss.common.registry.StorageNodeInfo;
import com.jay.oss.tracker.replica.ReplicaSelector;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

/**
 * <p>
 *
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

    public List<StorageNodeInfo> selectUploadNode(String key, long size, int replica) throws Exception {
        List<StorageNodeInfo> aliveNodes = registry.aliveNodes();
        return replicaSelector.select(aliveNodes, size, replica);
    }
}
