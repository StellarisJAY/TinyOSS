package com.jay.oss.common.kv;

import java.util.List;

/**
 * <p>
 *  KV存储接口
 * </p>
 *
 * @author Jay
 * @date 2022/05/05 10:35
 */
public interface KvStorage {
    /**
     * 初始化KV存储引擎
     * @throws Exception init error
     */
    void init() throws Exception;

    /**
     * KV存储get
     * @param key String
     * @return byte[]
     * @throws Exception get error
     */
    byte[] get(String key) throws Exception;

    /**
     * KV存储putIfAbsent
     * @param key key
     * @param value value
     * @return boolean put key success?
     */
    boolean putIfAbsent(String key, byte[] value);

    /**
     * KV存储put
     * @param key key
     * @param value value
     * @return boolean put success?
     */
    boolean put(String key, byte[] value);

    /**
     * KV存储delete
     * @param key key
     * @return boolean delete success?
     */
    boolean delete(String key);

    /**
     * list keys
     * @return List
     */
    List<String> keys();
}
