package com.jay.oss.common.util;

import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.registry.StorageNodeInfo;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

/**
 * <p>
 *  获取当前服务器的Storage信息
 * </p>
 *
 * @author Jay
 * @date 2022/02/21 10:49
 */
public class NodeInfoCollector {

    private static String ip = null;
    private static String localAddress = null;

    public static StorageNodeInfo getStorageNodeInfo(int port) {
        String path = OssConfigs.dataPath();
        File dir = new File(path);
        long usedSpace = FileUtils.sizeOfDirectory(dir);
        return StorageNodeInfo.builder()
                .url(getLocalAddress() + ":" + port)
                .space(dir.getUsableSpace())
                .usedSpace(usedSpace)
                .memoryTotal(Runtime.getRuntime().totalMemory())
                .available(true).build();
    }

    public static String getAddress() {
        if(localAddress == null){
            localAddress = getLocalAddress();
        }
        return localAddress + ":" + OssConfigs.port();
    }

    public static String getLocalAddress(){
        if(ip == null){
            try {
                for (Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();) {
                    NetworkInterface intf = en.nextElement();
                    String name = intf.getName();
                    if (!name.contains("docker") && !name.contains("lo")) {
                        for (Enumeration<InetAddress> enumIpAddr = intf.getInetAddresses(); enumIpAddr.hasMoreElements();) {
                            InetAddress inetAddress = enumIpAddr.nextElement();
                            if (!inetAddress.isLoopbackAddress()) {
                                String ipaddress = inetAddress.getHostAddress();
                                if (!ipaddress.contains("::") && !ipaddress.contains("0:0:") && !ipaddress.contains("fe80")) {
                                    ip = ipaddress;
                                }
                            }
                        }
                    }
                }
            } catch (SocketException ex) {
                ip = "127.0.0.1";
                ex.printStackTrace();
            }
        }
        return ip;
    }
}
