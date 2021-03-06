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
 *  StorageNode??????
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
            // commandHandler??????????????????
            ExecutorService commandHandlerExecutor = ThreadPoolUtil.newIoThreadPool("command-handler-worker-");
            // ???????????????
            this.commandHandler = new StorageNodeCommandHandler(client, commandFactory, commandHandlerExecutor, objectIndexManager, storageNodeProducer, blockManager);
            // FastOSS??????Dove?????????
            this.server = new DoveServer(new TinyOssCodec(), port, commandFactory);
            this.prometheusServer = new PrometheusServer();
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    private void init() throws Exception {
        Banner.printBanner();
        /*
            ???????????? ??? ????????????
         */
        ProtocolManager.registerProtocol(TinyOssProtocol.PROTOCOL_CODE, new TinyOssProtocol(commandHandler));
        SerializerManager.registerSerializer(OssConfigs.PROTOSTUFF_SERIALIZER, new ProtostuffSerializer());

        Map<Long, ObjectIndex> indexes = blockManager.loadBlocks();
        objectIndexManager.putIndexes(indexes);
        /*
            ??????????????????????????????
         */
        registry.init();
        StorageNodeInfo storageNodeInfo = NodeInfoCollector.getStorageNodeInfo(port);
        // ????????????storage????????????????????????ID
        List<Long> storedObjects = objectIndexManager.listObjectIds();
        // ???????????????Tracker???????????????????????????????????????storage??????????????????
        if(OssConfigs.enableTrackerRegistry()){
            storageNodeInfo.setStoredObjects(storedObjects);
        }
        registry.register(storageNodeInfo);
        // ??????Prometheus Gauge
        registerPrometheusGauge();

        if(!OssConfigs.enableTrackerMessaging()){
            String topicSuffix = "_" + NodeInfoCollector.getAddress().replace(":", "_");
            // ?????????Tracker??????????????????????????????
            storageNodeConsumer.subscribeTopic(OssConstants.DELETE_OBJECT_TOPIC + topicSuffix, new DeleteHandler(objectIndexManager, blockManager));
            storageNodeConsumer.subscribeTopic(OssConstants.REPLICA_TOPIC + topicSuffix, new ReplicaHandler(client, objectIndexManager, blockManager));
            StringJoiner idJoiner = new StringJoiner(";");
            storedObjects.forEach(id->{
                idJoiner.add(Long.toString(id));
            });
            // ????????????????????????storage?????????????????????
            storageNodeProducer.send(OssConstants.REPORT_TOPIC, NodeInfoCollector.getAddress(), idJoiner.toString());
        }
        // ????????????????????????
        Scheduler.scheduleAtFixedRate(new StorageNodeHeartBeatTask(objectIndexManager, registry ,storageTaskManager, port, storageNodeProducer),
                OssConfigs.ZOOKEEPER_SESSION_TIMEOUT,
                OssConfigs.ZOOKEEPER_SESSION_TIMEOUT, TimeUnit.MILLISECONDS);
        // ???6??????????????????block??????
        Scheduler.scheduleAtFixedMinutes(blockManager::compactBlocks, OssConfigs.blockCompactInterval(), OssConfigs.blockCompactInterval());
    }


    @Override
    public void startup() {
        super.startup();
        try{
            long start = System.currentTimeMillis();
            init();
            // ??????storage?????????
            server.startup();
            // ????????????????????????
            if(!OssConfigs.enableTrackerMessaging()){
                storageNodeConsumer.startup();
            }
            // ??????Prometheus??????
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
        prometheusServer.shutdown();
        if(!OssConfigs.enableTrackerMessaging()){
            storageNodeConsumer.shutdown();
        }
    }

    public static void main(String[] args) {
        StorageNode storageNode = new StorageNode("tiny-oss.conf");
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
