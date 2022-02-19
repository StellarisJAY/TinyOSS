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

    private static final String ZOOKEEPER_REGISTRY_HOST = "oss.registry.zookeeper.host";
    public static final int ZOOKEEPER_SESSION_TIMEOUT = 3 * 1000;

    private static final String DATA_PATH = "oss.data.path";
    public static final String DEFAULT_DATA_PATH = "./data";

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
}