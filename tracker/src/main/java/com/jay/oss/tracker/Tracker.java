package com.jay.oss.tracker;

import com.jay.dove.DoveServer;
import com.jay.dove.common.AbstractLifeCycle;
import com.jay.dove.serialize.SerializerManager;
import com.jay.dove.transport.protocol.ProtocolManager;
import com.jay.oss.common.config.ConfigsManager;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.constant.OssConstants;
import com.jay.oss.common.kafka.RecordConsumer;
import com.jay.oss.common.kafka.RecordProducer;
import com.jay.oss.common.kv.KvStorage;
import com.jay.oss.common.kv.bitcask.BitCaskStorage;
import com.jay.oss.common.prometheus.GaugeManager;
import com.jay.oss.common.prometheus.PrometheusServer;
import com.jay.oss.common.registry.Registry;
import com.jay.oss.common.registry.simple.SimpleRegistry;
import com.jay.oss.common.registry.zk.ZookeeperRegistry;
import com.jay.oss.common.remoting.TinyOssCodec;
import com.jay.oss.common.remoting.TinyOssCommandFactory;
import com.jay.oss.common.remoting.TinyOssProtocol;
import com.jay.oss.common.serialize.ProtostuffSerializer;
import com.jay.oss.common.util.Banner;
import com.jay.oss.tracker.kafka.handler.StorageUploadCompleteHandler;
import com.jay.oss.tracker.meta.BucketManager;
import com.jay.oss.tracker.registry.StorageNodeRegistry;
import com.jay.oss.tracker.remoting.TrackerCommandHandler;
import com.jay.oss.tracker.replica.SpaceBalancedReplicaSelector;
import com.jay.oss.tracker.task.StorageTaskManager;
import com.jay.oss.tracker.track.ObjectTracker;
import io.prometheus.client.Gauge;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 *  Tracker端主类
 * </p>
 *
 * @author Jay
 * @date 2022/02/21 10:08
 */
@Slf4j
public class Tracker extends AbstractLifeCycle {

    private final StorageNodeRegistry storageRegistry;
    private final BucketManager bucketManager;
    private final ObjectTracker objectTracker;
    private final KvStorage kvStorage;
    private final TrackerCommandHandler commandHandler;
    private final DoveServer server;
    private final Registry registry;
    private final StorageTaskManager storageTaskManager;
    private final RecordProducer trackerProducer;
    private final RecordConsumer trackerConsumer;
    private final PrometheusServer prometheusServer;

    public Tracker(){
        try{
            ConfigsManager.loadConfigs();
            int port = OssConfigs.port();
            TinyOssCommandFactory commandFactory = new TinyOssCommandFactory();

            this.kvStorage = new BitCaskStorage();
            this.bucketManager = new BucketManager(kvStorage);
            this.objectTracker = new ObjectTracker(kvStorage);

            this.trackerProducer = new RecordProducer();
            this.trackerConsumer = new RecordConsumer();
            this.storageTaskManager = new StorageTaskManager();
            if(OssConfigs.enableTrackerRegistry()){
                this.registry = new SimpleRegistry();
                this.storageRegistry = new StorageNodeRegistry(this.registry, new SpaceBalancedReplicaSelector());
                this.commandHandler = new TrackerCommandHandler(bucketManager, objectTracker, storageRegistry, trackerProducer,
                        (SimpleRegistry) registry, storageTaskManager, commandFactory);
            }else{
                this.registry = new ZookeeperRegistry();
                this.storageRegistry = new StorageNodeRegistry(this.registry, new SpaceBalancedReplicaSelector());
                this.commandHandler = new TrackerCommandHandler(bucketManager, objectTracker, storageRegistry, trackerProducer,
                        null, storageTaskManager, commandFactory);
            }
            this.server = new DoveServer(new TinyOssCodec(), port, commandFactory);
            this.prometheusServer = new PrometheusServer();
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    private void init() throws Exception {
        Banner.printBanner();
        // 协议添加默认command handler
        TinyOssProtocol fastOssProtocol = new TinyOssProtocol(this.commandHandler);
        // 注册协议
        ProtocolManager.registerProtocol(fastOssProtocol.getCode(), fastOssProtocol);
        // 注册默认序列化器
        ProtostuffSerializer serializer = new ProtostuffSerializer();
        SerializerManager.registerSerializer(OssConfigs.DEFAULT_SERIALIZER, serializer);
        // 注册Gauge
        registerGauges();
        kvStorage.init();
        // 初始化远程注册中心客户端
        registry.init();

        // 使用zookeeper注册中心时，需要初始化存储节点注册缓存
        if(!OssConfigs.enableTrackerRegistry()){
            storageRegistry.init();
        }
        // 使用消息队列时需要订阅主题
        if(OssConfigs.enableTrackerMessaging()){
            trackerConsumer.subscribeTopic(OssConstants.STORAGE_UPLOAD_COMPLETE, new StorageUploadCompleteHandler(trackerProducer, objectTracker));
        }
    }

    @Override
    public void startup() {
        super.startup();
        long start = System.currentTimeMillis();
        try{
            init();
            this.server.startup();
            this.trackerConsumer.startup();
            prometheusServer.startup();
            log.info("tracker started, time used: {}ms", (System.currentTimeMillis() - start));
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public void shutdown() {
        super.shutdown();
        this.server.shutdown();
    }

    public static void main(String[] args) {
        Tracker tracker = new Tracker();
        tracker.startup();
    }

    /**
     * 注册Prometheus监控gauge
     */
    private void registerGauges(){
        // 对象总数监控
        GaugeManager.registerGauge("object_count", Gauge.build()
                .name("object_count")
                .help("Show total object count in OSS System")
                .create());
    }
}
