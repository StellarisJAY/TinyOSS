package com.jay.oss.storage;

import com.jay.dove.DoveServer;
import com.jay.dove.common.AbstractLifeCycle;
import com.jay.dove.serialize.SerializerManager;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.protocol.ProtocolManager;
import com.jay.dove.util.NamedThreadFactory;
import com.jay.oss.common.OssConfigs;
import com.jay.oss.common.fs.ChunkManager;
import com.jay.oss.common.remoting.FastOssCodec;
import com.jay.oss.common.remoting.FastOssCommandFactory;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.serialize.ProtostuffSerializer;
import com.jay.oss.storage.command.StorageNodeCommandHandler;
import com.jay.oss.storage.meta.MetaManager;

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
 * @date 2022/01/27 10:39
 */
public class StorageNode extends AbstractLifeCycle {
    private final DoveServer server;
    private final MetaManager metaManager;
    private final ChunkManager chunkManager;
    private final StorageNodeCommandHandler commandHandler;

    public StorageNode(int port) {
        CommandFactory commandFactory = new FastOssCommandFactory();
        this.metaManager = new MetaManager();
        this.chunkManager = new ChunkManager();
        // commandHandler执行器线程池
        ExecutorService commandHandlerExecutor = new ThreadPoolExecutor(2 * Runtime.getRuntime().availableProcessors(),
                2 * Runtime.getRuntime().availableProcessors(),
                0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new NamedThreadFactory("command-handler-thread-", true));
        // StorageNodeCommandHandler
        this.commandHandler = new StorageNodeCommandHandler(commandFactory, commandHandlerExecutor, chunkManager, metaManager);
        // FastOSS协议Dove服务器
        this.server = new DoveServer(new FastOssCodec(), port, commandFactory);
    }

    private void init(){
        // 注册协议
        ProtocolManager.registerProtocol(FastOssProtocol.PROTOCOL_CODE, new FastOssProtocol(commandHandler));
        // 注册序列化器
        SerializerManager.registerSerializer(OssConfigs.PROTOSTUFF_SERIALIZER, new ProtostuffSerializer());
    }


    @Override
    public void startup() {
        super.startup();
        init();
        server.startup();
    }

    @Override
    public void shutdown() {
        super.shutdown();
        server.shutdown();
    }

    public static void main(String[] args) {
        StorageNode storageNode = new StorageNode(9999);
        storageNode.startup();
    }
}
