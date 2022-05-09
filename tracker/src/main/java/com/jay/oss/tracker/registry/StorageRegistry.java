package com.jay.oss.tracker.registry;

import com.jay.oss.common.registry.Registry;
import com.jay.oss.common.registry.StorageNodeInfo;
import com.jay.oss.tracker.replica.ReplicaSelector;
import com.jay.oss.tracker.replica.SpaceBalancedReplicaSelector;
import com.jay.oss.tracker.track.ConsistentHashRing;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 *  本地的storage节点注册记录
 * </p>
 *
 * @author Jay
 * @date 2022/02/21 10:05
 */
@Slf4j
public class StorageRegistry {
    private final ConcurrentHashMap<String, StorageNodeInfo> storages = new ConcurrentHashMap<>();
    private Registry registry;
    private final ConsistentHashRing ring;
    private final ReplicaSelector replicaSelector;
    public StorageRegistry(ConsistentHashRing ring) {
        this.ring = ring;
        this.replicaSelector = new SpaceBalancedReplicaSelector();
    }

    public void setRegistry(Registry registry) {
        this.registry = registry;
    }

    /**
     * 初始化本地存储节点记录
     * @throws Exception e
     */
    public void init() throws Exception {
        // 从远程注册中心获取所有storage信息
        Map<String, StorageNodeInfo> storageNodeInfoMap = registry.lookupAll();
        for (StorageNodeInfo node : storageNodeInfoMap.values()) {
            addStorageNode(node);
        }
    }

    public void addStorageNode(StorageNodeInfo node){
        storages.put(node.getUrl(), node);
        ring.addStorageNode(node);
    }

    /**
     * 选择上传点，通过一致性hash定位主副本位置
     * 然后使用ReplicaSelector选择备份位置
     * @param key object key
     * @param size object size
     * @param replica 副本数量，默认3副本
     * @throws Exception 节点不足
     * @return 上传点集合，第一个为主副本
     */
    public List<StorageNodeInfo> selectUploadNode(String key, long size, int replica) throws Exception {
        List<StorageNodeInfo> nodes = new ArrayList<>(storages.values());
        // 从一致性hash环定位主备份位置
        String mainReplica = ring.locateObject(key);
        // 选择备份节点
        List<StorageNodeInfo> result;
        result = replicaSelector.select(nodes, size, replica - 1);
        // 将主副本节点添加到列表头部
        result.add(0, storages.get(mainReplica));
        return result;
    }
}
