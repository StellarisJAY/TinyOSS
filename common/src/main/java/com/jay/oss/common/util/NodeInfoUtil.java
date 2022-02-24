package com.jay.oss.common.util;

import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.registry.StorageNodeInfo;

import java.io.File;
import java.net.Inet4Address;

/**
 * <p>
 *  获取当前服务器的Storage信息
 * </p>
 *
 * @author Jay
 * @date 2022/02/21 10:49
 */
public class NodeInfoUtil {
    public static StorageNodeInfo getStorageNodeInfo(int port) throws Exception {
        String path = OssConfigs.dataPath();
        File dir = new File(path);
        return StorageNodeInfo.builder()
                .url(Inet4Address.getLocalHost().getHostAddress() + ":" + port)
                .space(dir.getUsableSpace())
                .available(true).build();
    }
}
