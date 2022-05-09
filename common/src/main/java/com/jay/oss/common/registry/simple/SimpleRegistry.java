package com.jay.oss.common.registry.simple;

import com.jay.dove.DoveClient;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.registry.Registry;
import com.jay.oss.common.registry.StorageNodeInfo;
import com.jay.oss.common.remoting.TinyOssProtocol;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * <p>
 *  Tracker端注册中心
 * </p>
 *
 * @author Jay
 * @date 2022/05/09 9:46
 */
@Slf4j
public class SimpleRegistry implements Registry {
    private final ConcurrentHashMap<String, StorageNodeInfo> storageNodes = new ConcurrentHashMap<>(256);
    private DoveClient trackerClient;
    private boolean isTracker;
    public static final AttributeKey<String> STORAGE_NODE_ATTR = AttributeKey.valueOf("storage_node");

    public SimpleRegistry(DoveClient trackerClient){
        this.trackerClient = trackerClient;
        this.isTracker = false;
    }

    public SimpleRegistry(){
        this.trackerClient = null;
        this.isTracker = true;
    }

    @Override
    public void init() throws Exception {

    }

    @Override
    public void register(StorageNodeInfo storageNodeInfo) throws Exception {
        long startTime = System.currentTimeMillis();
        RemotingCommand command = createRegistryCommand(storageNodeInfo, TinyOssProtocol.REGISTER_STORAGE);
        RemotingCommand response = trackerClient.sendSync(OssConfigs.trackerServerUrl(), command, null);
        CommandCode code = response.getCommandCode();
        if(code.equals(TinyOssProtocol.SUCCESS)){
            log.info("Storage Node registered to Tracker time used : {}ms", (System.currentTimeMillis() - startTime));
        }else{
            throw new RuntimeException("Failed to register Storage Node");
        }
    }

    @Override
    public void update(StorageNodeInfo storageNodeInfo) throws Exception {
        RemotingCommand command = createRegistryCommand(storageNodeInfo, TinyOssProtocol.STORAGE_HEART_BEAT);
        RemotingCommand response = trackerClient.sendSync(OssConfigs.trackerServerUrl(), command, null);
        CommandCode code = response.getCommandCode();
        if(!code.equals(TinyOssProtocol.SUCCESS)){
            throw new RuntimeException("Failed to register Storage Node");
        }
    }

    @Override
    public Map<String, StorageNodeInfo> lookupAll() throws Exception {
        return storageNodes;
    }

    @Override
    public StorageNodeInfo lookup(String path) throws Exception {
        return storageNodes.get(path);
    }

    @Override
    public List<StorageNodeInfo> aliveNodes() {
        return storageNodes.values().stream()
                .filter(StorageNodeInfo::isAvailable)
                .collect(Collectors.toList());
    }

    private RemotingCommand createRegistryCommand(StorageNodeInfo storageNodeInfo, CommandCode code){
        return trackerClient.getCommandFactory()
                .createRequest(storageNodeInfo, code, StorageNodeInfo.class);
    }

    public void putStorageNode(StorageNodeInfo storageNodeInfo){
        storageNodes.put(storageNodeInfo.getUrl(), storageNodeInfo);
    }

    public void updateStorageNode(StorageNodeInfo storageNodeInfo){
        storageNodes.put(storageNodeInfo.getUrl(), storageNodeInfo);
    }

    public void setStorageNodeOffline(String url){
        storageNodes.computeIfPresent(url, (key, node)->{
            node.setAvailable(false);
            return node;
        });
    }


}
