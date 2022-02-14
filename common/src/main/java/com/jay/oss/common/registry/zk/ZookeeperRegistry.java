package com.jay.oss.common.registry.zk;

import com.alibaba.fastjson.JSON;
import com.jay.oss.common.registry.Registry;
import com.jay.oss.common.registry.StorageNodeInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * <p>
 *  Zookeeper 注册中心客户端
 * </p>
 *
 * @author Jay
 * @date 2022/02/14 10:37
 */
@Slf4j
public class ZookeeperRegistry implements Registry {
    private ZooKeeper zooKeeper;
    private static final String ROOT_PATH = "/fastOss/storages";
    @Override
    public void init() throws Exception{
        log.info("initializing zookeeper registry, host: {}", "127.0.0.1:2181");
        CountDownLatch countDownLatch = new CountDownLatch(1);
        long start = System.currentTimeMillis();
        // 建立Zookeeper连接
        zooKeeper = new ZooKeeper("127.0.0.1:2181", 4000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                // 异步监听连接事件
                if(watchedEvent.getState().equals(Event.KeeperState.SyncConnected)){
                    // 连接建立事件
                    if(watchedEvent.getPath() == null && watchedEvent.getType() == Event.EventType.None){
                        // countDown
                        countDownLatch.countDown();
                    }
                }
            }
        });
        // 同步等待
        countDownLatch.await();
        log.info("zookeeper connected, time used: {} ms", (System.currentTimeMillis() - start));
    }

    @Override
    public void register(StorageNodeInfo storageNodeInfo) throws Exception {
        try{
            // 创建根目录
            ensureRootPath();
            String group = storageNodeInfo.getGroup();
            if(zooKeeper.exists(ROOT_PATH + "/" + group, false) == null){
                zooKeeper.create(ROOT_PATH + "/" + group, group.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            // JSON序列化
            String json = JSON.toJSONString(storageNodeInfo);
            // 创建 临时节点
            zooKeeper.create(ROOT_PATH + "/" + group + "/" + storageNodeInfo.getUrl(), json.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            log.info("storage node registered, group: {}", group);
        }catch (Exception e){
            log.error("register node error ", e);
            throw e;
        }
    }


    /**
     * 保证根目录已创建
     * @throws Exception e
     */
    private void ensureRootPath() throws Exception{
        if(zooKeeper.exists("/fastOss", false) == null){
            zooKeeper.create("/fastOss", "FastOss v1.0".getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zooKeeper.create(ROOT_PATH, "FastOss v1.0/storages".getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    @Override
    public Map<String, List<StorageNodeInfo>> lookupAll() throws Exception {
        HashMap<String, List<StorageNodeInfo>> result = new HashMap<>();
        try{
            registerWatchers();
            List<String> groups = zooKeeper.getChildren(ROOT_PATH, false);
            for(String group : groups){
                result.put(group, lookupGroup(group));
            }

        }catch (Exception e){
            log.error("lookup storages error ", e);
            throw e;
        }
        return result;
    }

    private void registerWatchers() throws InterruptedException, KeeperException {
        zooKeeper.addWatch(ROOT_PATH, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                log.info("event: {}", watchedEvent);
            }
        }, AddWatchMode.PERSISTENT_RECURSIVE);
    }

    private ArrayList<StorageNodeInfo> lookupGroup(String group) throws Exception{
        List<String> children = zooKeeper.getChildren(ROOT_PATH + "/" + group, false);
        ArrayList<StorageNodeInfo> nodes = new ArrayList<>(children.size());
        for(String url: children){
            byte[] data = zooKeeper.getData(ROOT_PATH + "/" + group + "/" + url, false, new Stat());
            String json = new String(data, StandardCharsets.UTF_8);
            StorageNodeInfo node = JSON.parseObject(json, StorageNodeInfo.class);
            nodes.add(node);
        }
        return nodes;
    }


}
