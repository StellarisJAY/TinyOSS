package com.jay.oss.tracker.track;

import com.google.common.hash.Hashing;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.registry.StorageNodeInfo;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * <p>
 *  一致性HASH环
 *  管理object主备份与存储节点的映射关系
 * </p>
 *
 * @author Jay
 * @date 2022/02/23 11:12
 */
public class ConsistentHashRing {
    /**
     * 用TreeMap表示一致性HASH环
     */
    private final TreeMap<Integer, String> ring = new TreeMap<>();
    /**
     * TreeMap不是线程安全的，需要读写锁
     */
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    /**
     * 一致性HASH 虚节点个数
     */
    private static final int VIRTUAL_NODE_COUNT = 10;


    /**
     * 添加新的存储节点
     * @param node {@link StorageNodeInfo}
     */
    public void addStorageNode(StorageNodeInfo node){
        try{
            // 排他锁，保证Hash环的线程安全
            readWriteLock.writeLock().lock();
            String url = node.getUrl();
            int vnode = OssConfigs.vnodeCount();
            // 添加若干虚节点
            for(int i = 0; i < vnode; i++){
                // 虚节点hash
                int hash = hash(url + i);
                ring.put(hash, url);
            }
        }finally {
            readWriteLock.writeLock().unlock();
        }
    }

    /**
     * 定位object主备份位置
     * @param key object key 包含桶、版本号、备份号
     * @return url
     */
    public String locateObject(String key){
        try{
            // 共享锁，保证线程安全
            readWriteLock.readLock().lock();
            int hash = hash(key);
            NavigableMap<Integer, String> tailMap = ring.tailMap(hash, true);
            int nodeKey = tailMap.isEmpty() ? ring.firstKey() : tailMap.firstKey();
            return ring.get(nodeKey);
        }finally {
            readWriteLock.readLock().unlock();
        }
    }

    /**
     * 下线一个storage节点
     * @param url url
     */
    public void deleteStorageNode(String url){
        try{
            readWriteLock.writeLock().lock();
            // 删除所有虚拟节点
            for(int i = 0 ; i < VIRTUAL_NODE_COUNT ; i ++){
                int hash = hash(url + i);
                ring.remove(hash);
            }
        }finally {
            readWriteLock.writeLock().unlock();
        }
    }

    /**
     * test only
     */
    public void listNodes(){
        try{
            readWriteLock.readLock().lock();
            SortedMap<Integer, String> map = ring.headMap(Integer.MAX_VALUE);
            map.forEach((k, v)->System.out.println(v));
        }finally {
            readWriteLock.readLock().unlock();
        }
    }

    private int hash(String url){
        // murmur_128 hash
        long hash = Hashing.murmur3_128().hashString(url, StandardCharsets.UTF_8).asLong();
        // 将long变为32位int，后32位与前32位与
        return (int)(hash & (hash >>> 32));
    }
}
