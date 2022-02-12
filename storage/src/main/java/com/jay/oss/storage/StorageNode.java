package com.jay.oss.storage;

import com.jay.dove.DoveServer;
import com.jay.dove.common.AbstractLifeCycle;
import com.jay.dove.serialize.SerializerManager;
import com.jay.dove.transport.command.CommandFactory;
import com.jay.dove.transport.protocol.ProtocolManager;
import com.jay.oss.common.OssConfigs;
import com.jay.oss.common.fs.ChunkManager;
import com.jay.oss.common.remoting.FastOssCodec;
import com.jay.oss.common.remoting.FastOssCommandFactory;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.serialize.ProtostuffSerializer;
import com.jay.oss.common.util.Scheduler;
import com.jay.oss.common.util.ThreadPoolUtil;
import com.jay.oss.storage.command.StorageNodeCommandHandler;
import com.jay.oss.storage.meta.MetaManager;
import com.jay.oss.storage.persistence.Persistence;

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
    /**
     * 持久化工具
     */
    private final Persistence persistence;

    public StorageNode(int port) {
        CommandFactory commandFactory = new FastOssCommandFactory();
        this.metaManager = new MetaManager();
        this.chunkManager = new ChunkManager();
        this.persistence = new Persistence(metaManager, chunkManager);
        // commandHandler执行器线程池
        ExecutorService commandHandlerExecutor = ThreadPoolUtil.newIoThreadPool("command-handler-worker-");
        // 命令处理器
        this.commandHandler = new StorageNodeCommandHandler(commandFactory, commandHandlerExecutor, chunkManager, metaManager);
        // FastOSS协议Dove服务器
        this.server = new DoveServer(new FastOssCodec(), port, commandFactory);
    }

    private void init(){
        // 注册协议
        ProtocolManager.registerProtocol(FastOssProtocol.PROTOCOL_CODE, new FastOssProtocol(commandHandler));
        // 注册序列化器
        SerializerManager.registerSerializer(OssConfigs.PROTOSTUFF_SERIALIZER, new ProtostuffSerializer());
        // 加载持久化数据
        persistence.loadMeta();
        // 提交持久化定时任务
        Scheduler.scheduleAtFixedRate(persistence::persistenceMeta, 30, 30, TimeUnit.SECONDS);
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
