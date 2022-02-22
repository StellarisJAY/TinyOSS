package com.jay.oss.tracker;

import com.jay.dove.DoveServer;
import com.jay.dove.common.AbstractLifeCycle;
import com.jay.dove.serialize.SerializerManager;
import com.jay.dove.transport.protocol.ProtocolManager;
import com.jay.oss.common.config.ConfigsManager;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.registry.Registry;
import com.jay.oss.common.registry.zk.ZookeeperRegistry;
import com.jay.oss.common.remoting.FastOssCodec;
import com.jay.oss.common.remoting.FastOssCommandFactory;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.serialize.ProtostuffSerializer;
import com.jay.oss.tracker.meta.BucketManager;
import com.jay.oss.tracker.track.ObjectTracker;
import com.jay.oss.tracker.registry.StorageRegistry;
import com.jay.oss.tracker.remoting.TrackerCommandHandler;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 *
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

    private final TrackerCommandHandler commandHandler;

    private final DoveServer server;

    private final Registry registry;
    public Tracker(){
        ConfigsManager.loadConfigs();
        FastOssCommandFactory commandFactory = new FastOssCommandFactory();
        this.storageRegistry = new StorageRegistry();
        this.bucketManager = new BucketManager();
        this.objectTracker = new ObjectTracker();
        this.commandHandler = new TrackerCommandHandler(bucketManager, objectTracker, storageRegistry, commandFactory);
        this.registry = new ZookeeperRegistry();
        this.storageRegistry.setRegistry(registry);
        this.server = new DoveServer(new FastOssCodec(), 8000, commandFactory);
    }

    private void init() throws Exception {
        // 协议添加默认command handler
        FastOssProtocol fastOssProtocol = new FastOssProtocol(this.commandHandler);
        // 注册协议
        ProtocolManager.registerProtocol(fastOssProtocol.getCode(), fastOssProtocol);
        // 注册默认序列化器
        ProtostuffSerializer serializer = new ProtostuffSerializer();
        SerializerManager.registerSerializer(OssConfigs.DEFAULT_SERIALIZER, serializer);
        // 初始化远程注册中心客户端
        registry.init();
        // 初始化本地storage记录
        storageRegistry.init();
    }

    @Override
    public void startup() {
        super.startup();
        long start = System.currentTimeMillis();
        try{
            init();
            this.server.startup();
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
