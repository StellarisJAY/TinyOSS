package com.jay.oss.tracker.registry;

import com.jay.oss.common.registry.Registry;
import com.jay.oss.common.registry.StorageNodeInfo;
import com.jay.oss.tracker.meta.RandomStorageSelector;
import com.jay.oss.tracker.meta.StorageSelector;

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
public class StorageRegistry {

    private final ConcurrentHashMap<String, StorageNodeInfo> storages = new ConcurrentHashMap<>();
    private Registry registry;
    private StorageSelector storageSelector;

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
        storages.putAll(storageNodeInfoMap);
        this.storageSelector = new RandomStorageSelector();
    }

    public List<StorageNodeInfo> selectUploadNode(long size, int replica){
        List<StorageNodeInfo> nodes = new ArrayList<>(storages.values());
        return storageSelector.select(nodes, size, replica);
    }
}
