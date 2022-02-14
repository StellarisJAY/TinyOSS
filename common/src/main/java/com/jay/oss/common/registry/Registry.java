package com.jay.oss.common.registry;

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
     */
    void init() throws Exception;

    /**
     * 注册存储节点
     * @param storageNodeInfo {@link StorageNodeInfo}
     * @throws Exception register errors
     */
    void register(StorageNodeInfo storageNodeInfo) throws Exception;

    /**
     * 查询所有注册节点
     * @return {@link Map} key: groupName, value: List<Storages>
     */
    Map<String, List<StorageNodeInfo>> lookupAll() throws Exception;

}
