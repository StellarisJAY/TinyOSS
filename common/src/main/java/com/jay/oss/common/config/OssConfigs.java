package com.jay.oss.common.config;

import com.jay.dove.transport.Url;
import com.jay.oss.common.GzipCompressor;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

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

    public static final byte GZIP_COMPRESSOR = GzipCompressor.CODE;

    public static final Charset DEFAULT_CHARSET =  StandardCharsets.UTF_8;

    /**
     * 默认序列化器
     */
    public static final byte DEFAULT_SERIALIZER = PROTOSTUFF_SERIALIZER;
    public static final byte DEFAULT_COMPRESSOR = GZIP_COMPRESSOR;
    /**
     * Zookeeper 地址
     */
    private static final String ZOOKEEPER_REGISTRY_HOST = "oss.registry.zookeeper.host";
    public static final int ZOOKEEPER_SESSION_TIMEOUT = 10 * 1000;

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
    private static final int DEFAULT_REPLICA_COUNT = 1;

    /**
     * 一致性hash环，虚节点数量
     */
    private static final String VNODE_COUNT = "oss.load-balance.vnode";
    private static final int DEFAULT_VNODE_COUNT = 10;

    private static final String PROMETHEUS_SERVER_PORT = "oss.prometheus.port";
    private static final int DEFAULT_PROMETHEUS_PORT = 9898;

    private static final String EDIT_LOG_FLUSH_INTERVAL = "oss.log.flush_interval";
    private static final int DEFAULT_EDIT_LOG_FLUSH_INTERVAL = 20 * 1000;

    private static final String REPLICA_WRITE_RULE = "oss.replica.write-rule";
    private static final String DEFAULT_REPLICA_WRITE_RULE = "all";

    private static final String HTTP_MAX_REQUEST_BODY_SIZE = "oss.http.max-request-body";
    private static final int DEFAULT_HTTP_MAX_REQUEST_BODY_SIZE = 16 * 1024 * 1024;

    /**
     * 纠删码分片大小
     */
    private static final String RS_SHARD_SIZE = "oss.reed-solomon.shard-size";
    /**
     * 默认分片大小是,32MB，最小的RS存储对象是128MB，也就是说会产生4个数据分片
     */
    private static final int DEFAULT_RS_SHARD_SIZE = 32 * 1024 * 1024;

    /**
     * 使用纠删码替代副本的对象大小阈值
     */
    private static final String RS_THRESHOLD_SIZE = "oss.reed-solomon.threshold-size";
    /**
     * 默认阈值是128MB，只要对象大小大于等于128MB就会采用纠删码存储，而不是多副本存储
     */
    private static final int DEFAULT_RS_THRESHOLD_SIZE = 128 * 1024 * 1024;

    private static final String ENABLE_MYSQL = "oss.mysql.enable";
    private static final boolean DEFAULT_ENABLE_MYSQL = false;

    private static final String MYSQL_URL = "oss.mysql.url";
    private static final String DEFAULT_MYSQL_URL = "jdbc:mysql://127.0.0.1:3306/db_fastoss";

    private static final String MYSQL_USERNAME = "oss.mysql.username";
    private static final String DEFAULT_MYSQL_USERNAME = "root";

    private static final String MYSQL_PASSWORD = "oss.mysql.password";
    private static final String DEFAULT_MYSQL_PASSWORD = "";

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

    public static Url trackerServerUrl(){
        return Url.parseString(ConfigsManager.get(TRACKER_SERVER));
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

    public static int editLogFlushInterval(){
        return ConfigsManager.getInt(EDIT_LOG_FLUSH_INTERVAL, DEFAULT_EDIT_LOG_FLUSH_INTERVAL);
    }

    public static int maxRequestBodySize(){
        return ConfigsManager.getInt(HTTP_MAX_REQUEST_BODY_SIZE, DEFAULT_HTTP_MAX_REQUEST_BODY_SIZE);
    }

    public static int reedSolomonShardSize(){
        return ConfigsManager.getInt(RS_SHARD_SIZE, DEFAULT_RS_SHARD_SIZE);
    }

    public static int reedSolomonThreshold(){
        return ConfigsManager.getInt(RS_THRESHOLD_SIZE, DEFAULT_RS_THRESHOLD_SIZE);
    }

    public static String mysqlUrl(){
        return ConfigsManager.get(MYSQL_URL, DEFAULT_MYSQL_URL);
    }

    public static String mysqlUsername(){
        return ConfigsManager.get(MYSQL_USERNAME, DEFAULT_MYSQL_USERNAME);
    }

    public static String mysqlPassword(){
        return ConfigsManager.get(MYSQL_PASSWORD, DEFAULT_MYSQL_PASSWORD);
    }

    public static boolean enableMysql(){
        return ConfigsManager.getBoolean(ENABLE_MYSQL, DEFAULT_ENABLE_MYSQL);
    }
}
