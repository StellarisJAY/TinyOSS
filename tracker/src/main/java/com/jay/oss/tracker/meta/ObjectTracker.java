package com.jay.oss.tracker.meta;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/02/21 10:09
 */
public class ObjectTracker {

    /**
     * tracker 缓存
     * key: object key
     * value: map(版本 : 位置)
     *
     * 内存占用：
     * 假设平均每个key有2个版本，key平均长度64字节，三备份，url平均长度16字节
     * 对象头大小：String+Map+2*Long+2*List+6*String = 32 + 32 + 32 + 6 * 16 = 192 bytes
     * 数据大小：keyLen + 2 * long + 6 * url = 64 + 16 + 48 = 128 bytes
     * 总计：320bytes / key
     * 其中 60% 为 对象头 !!!
     * 百万量级：
     * 320 * 1M = 320MB
     *
     * 千万量级：
     * 320 * 10M = 3200MB !!!
     * 对象头占用了：3200 * 0.6 = 1920MB !!!
     *
     *
     * 必要对象头：
     * 三备份Url String，6 * 16 = 92bytes
     * key：16
     *
     */
    private final ConcurrentHashMap<String, String> objectCache = new ConcurrentHashMap<>();

}
