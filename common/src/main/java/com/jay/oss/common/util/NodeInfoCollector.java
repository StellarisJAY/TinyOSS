package com.jay.oss.common.util;

import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.registry.StorageNodeInfo;

import java.io.File;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <p>
 *  获取当前服务器的Storage信息
 * </p>
 *
 * @author Jay
 * @date 2022/02/21 10:49
 */
public class NodeInfoCollector {

    private static final AtomicLong IO_COUNTER = new AtomicLong(0);
    private static long lastIoCount = 0;

    public static StorageNodeInfo getStorageNodeInfo(int port) throws Exception {
        String path = OssConfigs.dataPath();
        File dir = new File(path);

        long ioCount = IO_COUNTER.get();
        long ioRate = ioCount - lastIoCount;
        lastIoCount = ioCount;
        return StorageNodeInfo.builder()
                .url(Inet4Address.getLocalHost().getHostAddress() + ":" + port)
                .space(dir.getUsableSpace())
                .memoryTotal(Runtime.getRuntime().totalMemory())
                .ioRate(ioRate)
                .available(true).build();
    }

    /**
     * 记录IO次数
     */
    public static void incrementIoCount(){
        IO_COUNTER.incrementAndGet();
    }

    public static String getAddress() throws UnknownHostException {
        return Inet4Address.getLocalHost().getHostAddress() + ":" + OssConfigs.port();
    }
}
