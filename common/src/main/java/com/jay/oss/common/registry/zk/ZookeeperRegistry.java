package com.jay.oss.common.registry.zk;

import com.alibaba.fastjson.JSON;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.registry.Registry;
import com.jay.oss.common.registry.StorageNodeInfo;
import com.jay.oss.common.util.ZkUtil;
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
 *  Storage根目录：/fast-oss/storages/{group}/{url}
 *  Storage信息：
 *  ./fxid： 文件事务ID
 *  ./bxid：存储桶事务ID
 *
 * </p>
 *
 * @author Jay
 * @date 2022/02/14 10:37
 */
@Slf4j
public class ZookeeperRegistry implements Registry {
    private ZooKeeper zooKeeper;
    private ZkUtil zkUtil;
    private static final String ROOT_PATH = "/fastOss/storages";
    @Override
    public void init() throws Exception{
        String host = OssConfigs.zookeeperHost();
        log.info("initializing zookeeper registry, host: {}", host);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        long start = System.currentTimeMillis();
        // 建立Zookeeper连接
        zooKeeper = new ZooKeeper(host, OssConfigs.ZOOKEEPER_SESSION_TIMEOUT, new Watcher() {
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
        this.zkUtil = new ZkUtil(zooKeeper);
        log.info("zookeeper connected, time used: {} ms", (System.currentTimeMillis() - start));
    }

    @Override
    public void register(StorageNodeInfo storageNodeInfo) throws Exception {
        try{
            // 创建根目录
            ensureRootPath();
            String group = storageNodeInfo.getGroup();
            String groupPath = ROOT_PATH + "/" + group;
            if(!zkUtil.exists(groupPath)){
                zkUtil.create(groupPath, group, false);
            }
            // JSON序列化
            String json = JSON.toJSONString(storageNodeInfo);
            String path = groupPath + "/" + storageNodeInfo.getUrl();
            // 创建 临时节点
            zkUtil.create(path, json, true);
            zkUtil.create(path + "/fxid", Long.toString(storageNodeInfo.getFxid()), true);
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
        if(!zkUtil.exists("/fastOss")){
            zkUtil.create("/fastOss", "FastOss v1.0", false);
            zkUtil.create(ROOT_PATH, "FastOss v1.0/storages", false);
        }
    }

    @Override
    public Map<String, List<StorageNodeInfo>> lookupAll() throws Exception {
        HashMap<String, List<StorageNodeInfo>> result = new HashMap<>();
        try{
            // 订阅node改变事件
            subscribeNodeChanges();
            // 获取当前存在的所有组
            List<String> groups = zooKeeper.getChildren(ROOT_PATH, false);
            // 查询每个组的节点信息
            for(String group : groups){
                result.put(group, lookupGroup(group));
            }

        }catch (Exception e){
            log.error("lookup storages error ", e);
            throw e;
        }
        return result;
    }

    /**
     * 订阅节点改变事件
     * @throws Exception e
     */
    private void subscribeNodeChanges() throws Exception {
        zkUtil.subscribe(ROOT_PATH, watchedEvent -> log.info("event: {}", watchedEvent));
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
