package com.jay.oss.common.registry.zk;

import com.alibaba.fastjson.JSON;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.registry.Registry;
import com.jay.oss.common.registry.StorageNodeInfo;
import com.jay.oss.common.util.ZkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

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
        if(!zkUtil.exists("/fastOss")){
            zkUtil.create("/fastOss", "FastOss v1.0", false);
            zkUtil.create(ROOT_PATH, "FastOss v1.0/storages", false);
        }
    }

    @Override
    public Map<String, StorageNodeInfo> lookupAll() throws Exception {
        HashMap<String, StorageNodeInfo> result = new HashMap<>(16);
        try{
            // 获取当前存在的所有storage
            List<String> nodes = zooKeeper.getChildren(ROOT_PATH, false);
            log.info("nodes: {}", nodes);
            // 遍历获取每个storage信息
            for(String path : nodes){
                String json = zkUtil.getData(ROOT_PATH + "/" + path);
                StorageNodeInfo nodeInfo = JSON.parseObject(json, StorageNodeInfo.class);
                int i = path.lastIndexOf("/");
                String url = path.substring(i + 1);
                result.put(url, nodeInfo);
            }
        }catch (Exception e){
            log.error("lookup storages error ", e);
            throw e;
        }
        return result;
    }

    @Override
    public StorageNodeInfo lookup(String path) throws Exception {
        String data = zkUtil.getData(path);
        return JSON.parseObject(data, StorageNodeInfo.class);
    }

    @Override
    public void subscribe(Watcher watcher) throws Exception{
        zkUtil.subscribe(ROOT_PATH, watcher);
    }


}
