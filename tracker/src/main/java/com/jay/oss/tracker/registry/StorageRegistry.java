package com.jay.oss.tracker.registry;

import com.jay.oss.common.registry.Registry;
import com.jay.oss.common.registry.StorageNodeInfo;
import com.jay.oss.tracker.track.RandomStorageSelector;
import com.jay.oss.tracker.track.StorageSelector;
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
        // 订阅节点更新事件
        registry.subscribe(new RemoteRegistryWatcher());
    }

    public void addStorageNode(StorageNodeInfo node){
        storages.put(node.getUrl(), node);
    }

    public List<StorageNodeInfo> selectUploadNode(long size, int replica){
        List<StorageNodeInfo> nodes = new ArrayList<>(storages.values());
        return storageSelector.select(nodes, size, replica);
    }

    class RemoteRegistryWatcher implements Watcher{
        @Override
        public void process(WatchedEvent watchedEvent) {
            if(watchedEvent.getState() == Event.KeeperState.SyncConnected){
                try{
                    String path = watchedEvent.getPath();
                    switch(watchedEvent.getType()){
                        case NodeDeleted: onNodeDeleted(path); break;
                        case NodeDataChanged: onNodeDataChanged(path); break;
                        case NodeChildrenChanged: onNodeChildrenChanged(path);break;
                        case NodeCreated: onNodeCreated(path); break;
                        default:break;
                    }
                }catch (Exception e){
                    log.warn("event watcher error: ", e);
                }
            }
        }

        private void onNodeDeleted(String path) throws Exception{
            log.info("node deleted: {}", path);
            int i = path.lastIndexOf("/");
            String url = path.substring(i + 1);
            StorageNodeInfo node = storages.get(url);
            if(node != null){
                node.setAvailable(false);
            }
        }

        private void onNodeDataChanged(String path) throws Exception{
            log.info("node data changed: {}", path);
            StorageNodeInfo node = registry.lookup(path);
            storages.put(node.getUrl(), node);
        }

        private void onNodeChildrenChanged(String path) throws Exception{
            log.info("node children changed: {}", path);
        }

        private void onNodeCreated(String path) throws Exception{
            log.info("node created: {}", path);
            StorageNodeInfo node = registry.lookup(path);
            addStorageNode(node);
        }
    }


}
