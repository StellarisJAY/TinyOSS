package com.jay.oss.common.registry;

import com.jay.oss.common.entity.response.StorageHeartBeatResponse;

import java.util.List;
import java.util.Map;

/**
 * <p>
 *  注册中心
 * </p>
 *
 * @author Jay
 * @date 2022/02/14 10:30
 */
public interface Registry {

    /**
     * 初始化注册中心客户端
     * 比如建立连接，发布订阅
     * @throws Exception e
     */
    void init() throws Exception;

    /**
     * 注册存储节点
     * @param storageNodeInfo {@link StorageNodeInfo}
     * @throws Exception register errors
     */
    void register(StorageNodeInfo storageNodeInfo) throws Exception;

    void update(StorageNodeInfo storageNodeInfo) throws Exception;

    /**
     * 心跳
     * @param storageNodeInfo {@link StorageNodeInfo}
     * @return {@link StorageHeartBeatResponse} Tracker端心跳回复
     * @throws Exception e
     */
    default StorageHeartBeatResponse trackerHeartBeat(StorageNodeInfo storageNodeInfo) throws Exception{
        return null;
    }

    /**
     * 查询所有注册节点
     * @throws Exception e
     * @return {@link Map} key: groupName, value: List<Storages>
     */
    Map<String, StorageNodeInfo> lookupAll() throws Exception;

    /**
     * 查询单个目录下的StorageNode
     * @param path path
     * @return {@link StorageNodeInfo}
     * @throws Exception e
     */
    StorageNodeInfo lookup(String path) throws Exception;

    /**
     * 获取所有存活的节点
     * @return {@link List<StorageNodeInfo>}
     */
    List<StorageNodeInfo> aliveNodes();

}
