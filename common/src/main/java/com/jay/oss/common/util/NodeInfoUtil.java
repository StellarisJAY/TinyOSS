package com.jay.oss.common.util;

import com.jay.oss.common.registry.StorageNodeInfo;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/02/21 10:49
 */
public class NodeInfoUtil {
    public static StorageNodeInfo getStorageNodeInfo(int port, long space){
        return StorageNodeInfo.builder()
                .url("127.0.0.1:" + port)
                .space(space).build();
    }
}
