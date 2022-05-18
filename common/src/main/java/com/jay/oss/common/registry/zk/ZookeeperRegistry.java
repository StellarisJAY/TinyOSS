package com.jay.oss.common.registry.zk;

import com.alibaba.fastjson.JSON;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.registry.Registry;
import com.jay.oss.common.registry.StorageNodeInfo;
import com.jay.oss.common.util.ZkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

/**
 * <p>
 *  Zookeeper 注册中心客户端
 *  Storage根目录：/fast-oss/storages/{url}
 *  Storage信息：
 *  ./fxid： 文件事务ID
 *
 * </p>
 *
 * @author Jay
 * @date 2022/02/14 10:37
 */
@Slf4j
public class ZookeeperRegistry implements Registry {
    private final ConcurrentHashMap<String, StorageNodeInfo> localCache = new ConcurrentHashMap<>(256);
    private ZooKeeper zooKeeper;
    private ZkUtil zkUtil;
    private static final String ROOT_PATH = "/tinyOss/storages";
    @Override
    public void init() throws Exception{
        String host = OssConfigs.zookeeperHost();
        log.info("initializing zookeeper registry, host: {}", host);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        long start = System.currentTimeMillis();
        // 建立Zookeeper连接
        zooKeeper = new ZooKeeper(host, OssConfigs.ZOOKEEPER_SESSION_TIMEOUT, watchedEvent -> {
            // 异步监听连接事件
            if(watchedEvent.getState().equals(Watcher.Event.KeeperState.SyncConnected)){
                // 连接建立事件
                if(watchedEvent.getPath() == null && watchedEvent.getType() == Watcher.Event.EventType.None){
                    // countDown
                    countDownLatch.countDown();
                }
            }
        });
        // 同步等待
        countDownLatch.await();
        this.zkUtil = new ZkUtil(zooKeeper);
        log.info("zookeeper connected, time used: {} ms", (System.currentTimeMillis() - start));
    }

    @Override
    public void register(StorageNodeInfo storageNodeInfo) throws Exception {
        try{
            // 创建根目录
            ensureRootPath();
            // JSON序列化
            String json = JSON.toJSONString(storageNodeInfo);
            String path = ROOT_PATH + "/" + storageNodeInfo.getUrl();
            // 创建 临时节点
            zkUtil.create(path, json, true);
            log.info("storage node registered to zookeeper");
        }catch (Exception e){
            log.error("register node error ", e);
            throw e;
        }
    }

    @Override
    public void update(StorageNodeInfo storageNodeInfo) throws Exception {
        String json = JSON.toJSONString(storageNodeInfo);
        zkUtil.setData(ROOT_PATH + "/" + storageNodeInfo.getUrl(), json);
    }


    /**
     * 保证根目录已创建
     * @throws Exception e
     */
    private void ensureRootPath() throws Exception{
        if(!zkUtil.exists("/tinyOss")){
            zkUtil.create("/tinyOss", "TinyOss v1.0", false);
            zkUtil.create(ROOT_PATH, "TinyOss v1.0/storages", false);
        }
    }

    @Override
    public Map<String, StorageNodeInfo> lookupAll() {
        if(localCache.isEmpty()){
            HashMap<String, StorageNodeInfo> result = new HashMap<>(16);
            try{
                // 获取当前存在的所有storage
                List<String> nodes = zooKeeper.getChildren(ROOT_PATH, false);
                log.info("online storage nodes: {}", nodes);
                // 遍历获取每个storage信息
                for(String path : nodes){
                    String json = zkUtil.getData(ROOT_PATH + "/" + path);
                    StorageNodeInfo nodeInfo = JSON.parseObject(json, StorageNodeInfo.class);
                    int i = path.lastIndexOf("/");
                    String url = path.substring(i + 1);
                    result.put(url, nodeInfo);
                }
                localCache.putAll(result);
                zkUtil.subscribe(ROOT_PATH, new RemoteRegistryWatcher());
            }catch (KeeperException.NoNodeException e){
                log.warn("No storage node online");
            }catch (Exception e){
                throw new RuntimeException(e);
            }
            return result;
        }
        return localCache;
    }

    @Override
    public StorageNodeInfo lookup(String path) throws Exception {
        String data = zkUtil.getData(path);
        return JSON.parseObject(data, StorageNodeInfo.class);
    }

    @Override
    public List<StorageNodeInfo> aliveNodes() {
        try{
            return lookupAll().values().stream()
                    .filter(StorageNodeInfo::isAvailable)
                    .collect(Collectors.toList());
        }catch (Exception e){
            return null;
        }
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

        /**
         * Node删除事件，即storage节点下线事件
         * @param path path
         */
        private void onNodeDeleted(String path){
            log.info("storage node offline: {}", path);
            int i = path.lastIndexOf("/");
            String url = path.substring(i + 1);
            // 设置节点状态为不可用
            StorageNodeInfo node = localCache.get(url);
            if(node != null){
                node.setAvailable(false);
            }
        }

        private void onNodeDataChanged(String path) throws Exception{
            StorageNodeInfo node = lookup(path);
            localCache.put(node.getUrl(), node);
        }

        private void onNodeChildrenChanged(String path){
            log.info("node children changed: {}", path);
        }

        /**
         * 新增node事件，即storage上线事件
         * @param path path
         * @throws Exception e
         */
        private void onNodeCreated(String path) throws Exception{
            log.info("storage node online: {}", path);
            // 注册表添加新节点
            StorageNodeInfo node = lookup(path);
            localCache.put(node.getUrl(), node);
        }
    }
}
