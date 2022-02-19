package com.jay.oss.common.config;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * <p>
 *  配置管理器
 * </p>
 *
 * @author Jay
 * @date 2022/02/14 14:31
 */
@Slf4j
public class ConfigsManager {
    private static Properties properties = new Properties();

    public static void loadConfigs() {
        String path = ConfigsManager.class.getClassLoader().getResource("com/jay/oss/common/config/ConfigsManager.class").getPath();
        int i = path.indexOf("!");
        if(i != -1){
            String jarPath = path.substring(0, i);
            String dir = jarPath.substring(6, jarPath.lastIndexOf("/"));
            File file = new File(dir + "/conf/fast-oss.conf");
            try(InputStream inputStream = new FileInputStream(file)){
                properties.load(inputStream);
            }catch (IOException e){
                log.error("failed to load configs from {}", "fast-oss.conf", e);
                throw new RuntimeException(e);
            }
        }else{
            try(InputStream stream = ConfigsManager.class.getClassLoader().getResourceAsStream("fast-oss.conf")){
                properties.load(stream);
            }catch (Exception e){
                log.error("failed to load configs from {}", "fast-oss.conf", e);
                throw new RuntimeException(e);
            }
        }


    }

    public static String get(String name){
        return properties.getProperty(name);
    }

    public static int getInt(String name){
        String s = get(name);
        return Integer.parseInt(s);
    }

    public static boolean getBool(String name){
        String s = get(name);
        return Boolean.parseBoolean(s);
    }
}