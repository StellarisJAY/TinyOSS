package com.jay.oss.storage;

import com.jay.dove.DoveServer;
import com.jay.dove.common.AbstractLifeCycle;
import com.jay.dove.serialize.SerializerManager;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.protocol.ProtocolManager;
import com.jay.oss.common.config.ConfigsManager;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.edit.EditLogManager;
import com.jay.oss.common.fs.ChunkManager;
import com.jay.oss.common.prometheus.PrometheusServer;
import com.jay.oss.common.registry.Registry;
import com.jay.oss.common.registry.zk.ZookeeperRegistry;
import com.jay.oss.common.remoting.FastOssCodec;
import com.jay.oss.common.remoting.FastOssCommandFactory;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.serialize.ProtostuffSerializer;
import com.jay.oss.common.util.Banner;
import com.jay.oss.common.util.NodeInfoUtil;
import com.jay.oss.common.util.Scheduler;
import com.jay.oss.common.util.ThreadPoolUtil;
import com.jay.oss.storage.command.StorageNodeCommandHandler;
import com.jay.oss.storage.edit.StorageEditLogManager;
import com.jay.oss.storage.meta.MetaManager;
import lombok.extern.slf4j.Slf4j;

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
    /**
     * TCP 服务器
     */
    private final DoveServer server;
    /**
     * meta管理器
     */
    private final MetaManager metaManager;
    /**
     * chunk管理器
     */
    private final ChunkManager chunkManager;
    /**
     * 命令处理器
     */
    private final StorageNodeCommandHandler commandHandler;

    private final EditLogManager editLogManager;
    private final Registry registry;
    private final PrometheusServer prometheusServer;
    private final int port;

    public StorageNode(int port) {
        ConfigsManager.loadConfigs();
        this.port = OssConfigs.port();
        CommandFactory commandFactory = new FastOssCommandFactory();
        this.metaManager = new MetaManager();
        this.chunkManager = new ChunkManager();
        this.editLogManager = new StorageEditLogManager();
        this.registry = new ZookeeperRegistry();
        // commandHandler执行器线程池
        ExecutorService commandHandlerExecutor = ThreadPoolUtil.newIoThreadPool("command-handler-worker-");
        // 命令处理器
        this.commandHandler = new StorageNodeCommandHandler(commandFactory, commandHandlerExecutor, chunkManager, metaManager, editLogManager);
        // FastOSS协议Dove服务器
        this.server = new DoveServer(new FastOssCodec(), port, commandFactory);
        this.prometheusServer = new PrometheusServer();
    }

    private void init() throws Exception {
        Banner.printBanner();
        // 注册协议
        ProtocolManager.registerProtocol(FastOssProtocol.PROTOCOL_CODE, new FastOssProtocol(commandHandler));
        // 注册序列化器
        SerializerManager.registerSerializer(OssConfigs.PROTOSTUFF_SERIALIZER, new ProtostuffSerializer());
        editLogManager.init();
        // 加载edit日志
        editLogManager.loadAndCompress(this.metaManager);
        // 初始化注册中心客户端
        registry.init();
        registry.register(NodeInfoUtil.getStorageNodeInfo(port));
        // 提交定时汇报任务
        Scheduler.scheduleAtFixedRate(()->{
            try{
                registry.update(NodeInfoUtil.getStorageNodeInfo(port));
            }catch (Exception e){
                log.warn("update storage node info error ", e);
            }
        }, 5000, 5000, TimeUnit.MILLISECONDS);
    }


    @Override
    public void startup() {
        super.startup();
        try{
            long start = System.currentTimeMillis();
            init();
            server.startup();
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
    }

    public static void main(String[] args) {
        ConfigsManager.loadConfigs();
        StorageNode storageNode = new StorageNode(OssConfigs.port());
        storageNode.startup();
    }
}
