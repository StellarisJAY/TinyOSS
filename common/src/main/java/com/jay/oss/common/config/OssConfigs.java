package com.jay.oss.common.config;

/**
 * <p>
 *  FastOSS默认配置
 * </p>
 *
 * @author Jay
 * @date 2022/01/25 10:45
 */
public class OssConfigs {

    /**
     * protostuff序列化器
     */
    public static final byte PROTOSTUFF_SERIALIZER = 1;

    /**
     * 默认序列化器
     */
    public static final byte DEFAULT_SERIALIZER = PROTOSTUFF_SERIALIZER;
    /**
     * Zookeeper 地址
     */
    private static final String ZOOKEEPER_REGISTRY_HOST = "oss.registry.zookeeper.host";
    public static final int ZOOKEEPER_SESSION_TIMEOUT = 3 * 1000;

    /**
     * 数据目录
     */
    private static final String DATA_PATH = "oss.data.path";
    public static final String DEFAULT_DATA_PATH = "./data";

    /**
     * Tracker服务器 地址
     */
    private static final String TRACKER_SERVER = "oss.tracker.host";

    /**
     * 副本数量
     */
    private static final String REPLICA = "oss.replica.count";
    private static final int DEFAULT_REPLICA_COUNT = 3;

    /**
     * 一致性hash环，虚节点数量
     */
    private static final String VNODE_COUNT = "oss.load-balance.vnode";
    private static final int DEFAULT_VNODE_COUNT = 10;

    private static final String PROMETHEUS_SERVER_PORT = "oss.prometheus.port";
    private static final int DEFAULT_PROMETHEUS_PORT = 9898;

    public static String zookeeperHost(){
        return ConfigsManager.get(ZOOKEEPER_REGISTRY_HOST);
    }

    public static String dataPath(){
        String s;
        return ((s = ConfigsManager.get(DATA_PATH)) == null ? DEFAULT_DATA_PATH : s);
    }

    public static int port(){
        return ConfigsManager.getInt("server.port");
    }

    public static String trackerServerHost(){
        return ConfigsManager.get(TRACKER_SERVER);
    }

    public static int replicaCount(){
        return ConfigsManager.getInt(REPLICA, DEFAULT_REPLICA_COUNT);
    }

    public static int vnodeCount(){
        return ConfigsManager.getInt(VNODE_COUNT, DEFAULT_VNODE_COUNT);
    }

    public static int prometheusServerPort(){
        return ConfigsManager.getInt(PROMETHEUS_SERVER_PORT, DEFAULT_PROMETHEUS_PORT);
    }
}
