package com.jay.oss.proxy;

import com.jay.dove.DoveClient;
import com.jay.dove.common.AbstractLifeCycle;
import com.jay.dove.serialize.SerializerManager;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.command.CommandHandler;
import com.jay.dove.transport.connection.ConnectionFactory;
import com.jay.dove.transport.connection.ConnectionManager;
import com.jay.dove.transport.protocol.ProtocolManager;
import com.jay.dove.util.NamedThreadFactory;
import com.jay.oss.common.config.ConfigsManager;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.registry.Registry;
import com.jay.oss.common.registry.StorageNodeInfo;
import com.jay.oss.common.registry.zk.ZookeeperRegistry;
import com.jay.oss.common.remoting.FastOssCommandFactory;
import com.jay.oss.common.remoting.FastOssCommandHandler;
import com.jay.oss.common.remoting.FastOssConnectionFactory;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.serialize.ProtostuffSerializer;
import com.jay.oss.common.util.Banner;
import com.jay.oss.proxy.handler.BucketHandler;
import com.jay.oss.proxy.handler.ObjectHandler;
import com.jay.oss.proxy.http.HttpServer;
import com.jay.oss.proxy.http.handler.HandlerMapping;
import com.jay.oss.proxy.service.BucketService;
import com.jay.oss.proxy.service.DownloadService;
import com.jay.oss.proxy.service.ObjectService;
import com.jay.oss.proxy.service.UploadService;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/01/25 11:32
 */
@Slf4j
public class ProxyNode extends AbstractLifeCycle {

    /**
     * Proxy节点HTTP服务器
     */
    private final HttpServer httpServer;
    /**
     * 存储节点访问客户端
     */
    private final DoveClient storageClient;
    private final UploadService uploadService;
    private final DownloadService downloadService;
    private final ObjectService objectService;
    private final BucketService bucketService;

    private final CommandHandler commandHandler;
    public ProxyNode() {
        httpServer = new HttpServer();
        CommandFactory commandFactory = new FastOssCommandFactory();
        ConnectionFactory connectionFactory = new FastOssConnectionFactory();
        ConnectionManager connectionManager = new ConnectionManager(connectionFactory);
        // commandHandler执行器线程池
        ExecutorService commandHandlerExecutor = new ThreadPoolExecutor(2 * Runtime.getRuntime().availableProcessors(),
                2 * Runtime.getRuntime().availableProcessors(),
                0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new NamedThreadFactory("command-handler-thread-", true));
        // command handler
        commandHandler = new FastOssCommandHandler(commandFactory, commandHandlerExecutor);
        // 存储节点客户端
        storageClient = new DoveClient(connectionManager, commandFactory);
        uploadService = new UploadService(storageClient);
        downloadService = new DownloadService(storageClient);
        objectService = new ObjectService(storageClient);
        bucketService = new BucketService(storageClient);
    }

    private void init() throws Exception {
        Banner.printBanner();
        // 注册序列化器
        SerializerManager.registerSerializer(OssConfigs.PROTOSTUFF_SERIALIZER, new ProtostuffSerializer());
        // 注册FastOSS协议
        ProtocolManager.registerProtocol(FastOssProtocol.PROTOCOL_CODE, new FastOssProtocol(commandHandler));
        // 注册handler
        HandlerMapping.registerHandler("object", new ObjectHandler(uploadService, downloadService, objectService));
        HandlerMapping.registerHandler("bucket", new BucketHandler(bucketService));
    }

    @Override
    public void startup() {
        super.startup();
        try{
            long start = System.currentTimeMillis();
            init();
            httpServer.startup();
            log.info("Proxy Node started, time used: {} ms", (System.currentTimeMillis() - start));
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public void shutdown() {
        super.shutdown();
        httpServer.shutdown();
    }

    public static void main(String[] args) {
        ConfigsManager.loadConfigs();
        ProxyNode proxyNode = new ProxyNode();
        proxyNode.startup();
    }
}
