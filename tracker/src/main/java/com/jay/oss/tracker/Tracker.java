package com.jay.oss.tracker;

import com.jay.dove.DoveServer;
import com.jay.dove.common.AbstractLifeCycle;
import com.jay.dove.serialize.SerializerManager;
import com.jay.dove.transport.protocol.ProtocolManager;
import com.jay.oss.common.config.ConfigsManager;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.edit.EditLogManager;
import com.jay.oss.common.prometheus.PrometheusServer;
import com.jay.oss.common.registry.Registry;
import com.jay.oss.common.registry.zk.ZookeeperRegistry;
import com.jay.oss.common.remoting.FastOssCodec;
import com.jay.oss.common.remoting.FastOssCommandFactory;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.serialize.ProtostuffSerializer;
import com.jay.oss.common.util.Banner;
import com.jay.oss.common.util.Scheduler;
import com.jay.oss.tracker.edit.TrackerEditLogManager;
import com.jay.oss.tracker.meta.BucketManager;
import com.jay.oss.tracker.track.ConsistentHashRing;
import com.jay.oss.tracker.track.MultipartUploadTracker;
import com.jay.oss.tracker.track.ObjectTracker;
import com.jay.oss.tracker.registry.StorageRegistry;
import com.jay.oss.tracker.remoting.TrackerCommandHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

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

    private final StorageRegistry storageRegistry;
    private final BucketManager bucketManager;
    private final ObjectTracker objectTracker;
    private final MultipartUploadTracker multipartUploadTracker;
    private final TrackerCommandHandler commandHandler;
    private final DoveServer server;
    private final Registry registry;
    private final ConsistentHashRing ring;
    private final EditLogManager editLogManager;
    private final PrometheusServer prometheusServer;

    public Tracker(){
        ConfigsManager.loadConfigs();
        int port = OssConfigs.port();
        FastOssCommandFactory commandFactory = new FastOssCommandFactory();
        this.ring = new ConsistentHashRing();
        this.storageRegistry = new StorageRegistry(ring);
        this.bucketManager = new BucketManager();
        this.objectTracker = new ObjectTracker();
        this.multipartUploadTracker = new MultipartUploadTracker();
        this.editLogManager = new TrackerEditLogManager(objectTracker, bucketManager, multipartUploadTracker);
        this.commandHandler = new TrackerCommandHandler(bucketManager, objectTracker, storageRegistry, editLogManager, multipartUploadTracker, commandFactory);
        this.registry = new ZookeeperRegistry();
        this.storageRegistry.setRegistry(registry);
        this.server = new DoveServer(new FastOssCodec(), port, commandFactory);
        this.prometheusServer = new PrometheusServer();
    }

    private void init() throws Exception {
        Banner.printBanner();
        // 协议添加默认command handler
        FastOssProtocol fastOssProtocol = new FastOssProtocol(this.commandHandler);
        // 注册协议
        ProtocolManager.registerProtocol(fastOssProtocol.getCode(), fastOssProtocol);
        // 注册默认序列化器
        ProtostuffSerializer serializer = new ProtostuffSerializer();
        SerializerManager.registerSerializer(OssConfigs.DEFAULT_SERIALIZER, serializer);
        // 初始化 并 加载编辑日志
        editLogManager.init();
        // 初始化objectTracker，加载bitCask chunks
        objectTracker.init();
        // 加载editLog并压缩日志，该过程会压缩bitCask chunk
        editLogManager.loadAndCompress();
        // 初始化远程注册中心客户端
        registry.init();
        // 初始化本地storage记录
        storageRegistry.init();

        // 系统关闭hook，关闭时flush日志
        Runtime.getRuntime().addShutdownHook(new Thread(()-> {editLogManager.swapBuffer(true);editLogManager.close();}, "shutdown-log-flush"));
        // 定时flush任务
        Scheduler.scheduleAtFixedRate(()->editLogManager.swapBuffer(true), OssConfigs.editLogFlushInterval(), OssConfigs.editLogFlushInterval(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void startup() {
        super.startup();
        long start = System.currentTimeMillis();
        try{
            init();
            this.server.startup();
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
}
