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
import com.jay.oss.common.edit.EditLogManager;
import com.jay.oss.common.kafka.RecordConsumer;
import com.jay.oss.common.kafka.RecordProducer;
import com.jay.oss.common.prometheus.GaugeManager;
import com.jay.oss.common.prometheus.PrometheusServer;
import com.jay.oss.common.registry.Registry;
import com.jay.oss.common.registry.StorageNodeInfo;
import com.jay.oss.common.registry.zk.ZookeeperRegistry;
import com.jay.oss.common.remoting.FastOssCodec;
import com.jay.oss.common.remoting.FastOssCommandFactory;
import com.jay.oss.common.remoting.FastOssConnectionFactory;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.serialize.ProtostuffSerializer;
import com.jay.oss.common.util.Banner;
import com.jay.oss.common.util.NodeInfoCollector;
import com.jay.oss.common.util.Scheduler;
import com.jay.oss.common.util.ThreadPoolUtil;
import com.jay.oss.storage.command.StorageNodeCommandHandler;
import com.jay.oss.storage.edit.StorageEditLogManager;
import com.jay.oss.storage.fs.BlockManager;
import com.jay.oss.storage.fs.ChunkManager;
import com.jay.oss.storage.fs.ObjectIndex;
import com.jay.oss.storage.kafka.handler.DeleteHandler;
import com.jay.oss.storage.kafka.handler.ReplicaHandler;
import com.jay.oss.storage.meta.MetaManager;
import io.prometheus.client.Gauge;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
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
    private final MetaManager metaManager;
    private final ChunkManager chunkManager;
    private final BlockManager blockManager;
    private final StorageNodeCommandHandler commandHandler;
    private final EditLogManager editLogManager;
    private final Registry registry;
    private final RecordConsumer storageNodeConsumer;
    private final RecordProducer storageNodeProducer;
    private final PrometheusServer prometheusServer;
    private final int port;

    public StorageNode(String configPath) {
        try{
            ConfigsManager.loadConfigs(configPath);
            this.port = OssConfigs.port();
            CommandFactory commandFactory = new FastOssCommandFactory();
            FastOssConnectionFactory connectionFactory = new FastOssConnectionFactory();
            ConnectionManager connectionManager = new ConnectionManager(connectionFactory);
            this.client = new DoveClient(connectionManager, commandFactory);
            this.metaManager = new MetaManager();
            this.chunkManager = new ChunkManager();
            this.blockManager = new BlockManager();
            this.editLogManager = new StorageEditLogManager(metaManager, chunkManager);
            this.registry = new ZookeeperRegistry();
            this.storageNodeConsumer = new RecordConsumer();
            this.storageNodeProducer = new RecordProducer();
            // commandHandler执行器线程池
            ExecutorService commandHandlerExecutor = ThreadPoolUtil.newIoThreadPool("command-handler-worker-");
            // 命令处理器
            this.commandHandler = new StorageNodeCommandHandler(commandFactory, commandHandlerExecutor,
                    chunkManager, metaManager, editLogManager, storageNodeProducer, blockManager);
            // FastOSS协议Dove服务器
            this.server = new DoveServer(new FastOssCodec(), port, commandFactory);
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
        ProtocolManager.registerProtocol(FastOssProtocol.PROTOCOL_CODE, new FastOssProtocol(commandHandler));
        SerializerManager.registerSerializer(OssConfigs.PROTOSTUFF_SERIALIZER, new ProtostuffSerializer());

        Map<Long, ObjectIndex> indexes = blockManager.loadBlocks();
        metaManager.putIndexes(indexes);
        /*
            初始化注册中心客户端
         */
        registry.init();
        registry.register(NodeInfoCollector.getStorageNodeInfo(port));

        registerPrometheusGauge();
        /*
            订阅消息主题
         */
        storageNodeConsumer.subscribeTopic(OssConstants.DELETE_OBJECT_TOPIC, new DeleteHandler(metaManager, blockManager));
        storageNodeConsumer.subscribeTopic(OssConstants.REPLICA_TOPIC, new ReplicaHandler(client, chunkManager, editLogManager, metaManager));
        // 提交定时汇报任务
        Scheduler.scheduleAtFixedRate(()->{
            try{
                StorageNodeInfo storageNodeInfo = NodeInfoCollector.getStorageNodeInfo(port);
                registry.update(storageNodeInfo);
                // 更新存储容量监控数据
                GaugeManager.getGauge("storage_used").set(storageNodeInfo.getUsedSpace());
                GaugeManager.getGauge("storage_free").set(storageNodeInfo.getSpace());
            }catch (Exception e){
                log.warn("update storage node info error ", e);
            }
        }, OssConfigs.ZOOKEEPER_SESSION_TIMEOUT, OssConfigs.ZOOKEEPER_SESSION_TIMEOUT, TimeUnit.MILLISECONDS);

        Scheduler.scheduleAtFixedRate(()->editLogManager.swapBuffer(true),
                OssConfigs.editLogFlushInterval(),
                OssConfigs.editLogFlushInterval(),
                TimeUnit.MILLISECONDS);
        // 系统关闭hook，关闭时flush日志
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            editLogManager.swapBuffer(true);
            editLogManager.close();
        }, "shutdown-log-flush"));
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
