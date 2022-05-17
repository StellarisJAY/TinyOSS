package com.jay.oss.storage;

import com.jay.dove.DoveClient;
import com.jay.dove.DoveServer;
import com.jay.dove.common.AbstractLifeCycle;
import com.jay.dove.serialize.SerializerManager;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.connection.ConnectionManager;
import com.jay.dove.transport.protocol.ProtocolManager;
import com.jay.oss.common.config.ConfigsManager;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.constant.OssConstants;
import com.jay.oss.common.kafka.RecordConsumer;
import com.jay.oss.common.kafka.RecordProducer;
import com.jay.oss.common.prometheus.GaugeManager;
import com.jay.oss.common.prometheus.PrometheusServer;
import com.jay.oss.common.registry.Registry;
import com.jay.oss.common.registry.StorageNodeInfo;
import com.jay.oss.common.registry.simple.SimpleRegistry;
import com.jay.oss.common.registry.zk.ZookeeperRegistry;
import com.jay.oss.common.remoting.TinyOssCodec;
import com.jay.oss.common.remoting.TinyOssCommandFactory;
import com.jay.oss.common.remoting.TinyOssConnectionFactory;
import com.jay.oss.common.remoting.TinyOssProtocol;
import com.jay.oss.common.serialize.ProtostuffSerializer;
import com.jay.oss.common.util.Banner;
import com.jay.oss.common.util.NodeInfoCollector;
import com.jay.oss.common.util.Scheduler;
import com.jay.oss.common.util.ThreadPoolUtil;
import com.jay.oss.storage.command.StorageNodeCommandHandler;
import com.jay.oss.storage.fs.BlockManager;
import com.jay.oss.storage.fs.ObjectIndex;
import com.jay.oss.storage.fs.ObjectIndexManager;
import com.jay.oss.storage.kafka.handler.DeleteHandler;
import com.jay.oss.storage.kafka.handler.ReplicaHandler;
import com.jay.oss.storage.task.StorageNodeHeartBeatTask;
import com.jay.oss.storage.task.StorageTaskManager;
import io.prometheus.client.Gauge;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  StorageNode主类
 * </p>
 *
 * @author Jay
 * @date 2022/01/27 10:39
 */
@Slf4j
public class StorageNode extends AbstractLifeCycle {

    private final DoveServer server;
    private final DoveClient client;
    private final ObjectIndexManager objectIndexManager;
    private final BlockManager blockManager;
    private final StorageNodeCommandHandler commandHandler;
    private final Registry registry;
    private final StorageTaskManager storageTaskManager;
    private final RecordConsumer storageNodeConsumer;
    private final RecordProducer storageNodeProducer;
    private final PrometheusServer prometheusServer;
    private final int port;

    public StorageNode(String configPath) {
        try{
            ConfigsManager.loadConfigs(configPath);
            this.port = OssConfigs.port();
            CommandFactory commandFactory = new TinyOssCommandFactory();
            TinyOssConnectionFactory connectionFactory = new TinyOssConnectionFactory();
            ConnectionManager connectionManager = new ConnectionManager(connectionFactory);
            this.client = new DoveClient(connectionManager, commandFactory);
            this.objectIndexManager = new ObjectIndexManager();
            this.blockManager = new BlockManager(objectIndexManager);

            if(OssConfigs.enableTrackerRegistry()){
                this.registry = new SimpleRegistry(this.client);
            }
            else{
                this.registry = new ZookeeperRegistry();
            }
            this.storageTaskManager = new StorageTaskManager(client, blockManager, objectIndexManager);
            this.storageNodeConsumer = new RecordConsumer();
            this.storageNodeProducer = new RecordProducer();
            // commandHandler执行器线程池
            ExecutorService commandHandlerExecutor = ThreadPoolUtil.newIoThreadPool("command-handler-worker-");
            // 命令处理器
            this.commandHandler = new StorageNodeCommandHandler(client, commandFactory, commandHandlerExecutor, objectIndexManager, storageNodeProducer, blockManager);
            // FastOSS协议Dove服务器
            this.server = new DoveServer(new TinyOssCodec(), port, commandFactory);
            this.prometheusServer = new PrometheusServer();
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    private void init() throws Exception {
        Banner.printBanner();
        /*
            注册协议 和 序列化器
         */
        ProtocolManager.registerProtocol(TinyOssProtocol.PROTOCOL_CODE, new TinyOssProtocol(commandHandler));
        SerializerManager.registerSerializer(OssConfigs.PROTOSTUFF_SERIALIZER, new ProtostuffSerializer());

        Map<Long, ObjectIndex> indexes = blockManager.loadBlocks();
        objectIndexManager.putIndexes(indexes);
        /*
            初始化注册中心客户端
         */
        registry.init();
        StorageNodeInfo storageNodeInfo = NodeInfoCollector.getStorageNodeInfo(port);
        // 获取当前storage节点上的所有对象ID
        List<Long> storedObjects = objectIndexManager.listObjectIds();
        // 如果开启了Tracker心跳，使用注册报文发送当前storage上保存的对象
        if(OssConfigs.enableTrackerRegistry()){
            storageNodeInfo.setStoredObjects(storedObjects);
        }
        registry.register(storageNodeInfo);
        // 注册Prometheus Gauge
        registerPrometheusGauge();

        if(!OssConfigs.enableTrackerMessaging()){
            // 非全能Tracker模式下，订阅消息主题
            storageNodeConsumer.subscribeTopic(OssConstants.DELETE_OBJECT_TOPIC, new DeleteHandler(objectIndexManager, blockManager));
            storageNodeConsumer.subscribeTopic(OssConstants.REPLICA_TOPIC + "_" + NodeInfoCollector.getAddress().replace(":", "_"), new ReplicaHandler(client, objectIndexManager, blockManager));
            StringJoiner idJoiner = new StringJoiner(";");
            storedObjects.forEach(id->{
                idJoiner.add(Long.toString(id));
            });
            // 通过消息队列发送storage保存的对象列表
            storageNodeProducer.send(OssConstants.REPORT_TOPIC, NodeInfoCollector.getAddress(), idJoiner.toString());
        }
        // 提交定时汇报任务
        Scheduler.scheduleAtFixedRate(new StorageNodeHeartBeatTask(objectIndexManager, registry ,storageTaskManager, port, storageNodeProducer),
                OssConfigs.ZOOKEEPER_SESSION_TIMEOUT,
                OssConfigs.ZOOKEEPER_SESSION_TIMEOUT, TimeUnit.MILLISECONDS);
        // 没6小时尝试压缩block文件
        Scheduler.scheduleAtFixedMinutes(blockManager::compactBlocks, OssConfigs.blockCompactInterval(), OssConfigs.blockCompactInterval());
    }


    @Override
    public void startup() {
        super.startup();
        try{
            long start = System.currentTimeMillis();
            init();
            // 启动storage服务器
            server.startup();
            // 启动消息订阅循环
            storageNodeConsumer.startup();
            // 启动Prometheus监控
            prometheusServer.startup();
            log.info("Storage Node started, time used: {} ms", (System.currentTimeMillis() - start));
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public void shutdown() {
        super.shutdown();
        server.shutdown();
        storageNodeConsumer.shutdown();
    }

    public static void main(String[] args) {
        StorageNode storageNode = new StorageNode("fast-oss.conf");
        storageNode.startup();
    }

    private void registerPrometheusGauge(){
        GaugeManager.registerGauge("storage_used", Gauge.build()
                .name("storage_used")
                .help("Show storage node used disk space, unit: bytes")
                .create());
        GaugeManager.registerGauge("storage_free", Gauge.build()
                .name("storage_free")
                .help("Show storage node used disk free space, unit: bytes")
                .create());
    }
}
