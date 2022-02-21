package com.jay.oss.common.util;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/02/20 10:46
 */
public class ZkUtil {

    private final ZooKeeper zooKeeper;

    public ZkUtil(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    public void create(String path, String data, boolean ephemeral) throws Exception {
        zooKeeper.create(path, data.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, ephemeral? CreateMode.EPHEMERAL : CreateMode.PERSISTENT);
    }

    public String getData(String path) throws Exception{
        return new String(zooKeeper.getData(path, false, new Stat()), StandardCharsets.UTF_8);
    }

    public boolean exists(String path) throws Exception{
        return zooKeeper.exists(path, false) != null;
    }

    public void subscribe(String path, Watcher watcher) throws Exception{
        zooKeeper.addWatch(path, watcher, AddWatchMode.PERSISTENT_RECURSIVE);
    }
}
