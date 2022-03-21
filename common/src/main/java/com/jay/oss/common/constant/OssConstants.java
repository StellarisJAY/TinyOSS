package com.jay.oss.common.constant;

/**
 * <p>
 *  Oss常量
 * </p>
 *
 * @author Jay
 * @date 2022/03/21 10:19
 */
public class OssConstants {
    /**
     * 删除对象消息主题
     */
    public static final String DELETE_OBJECT_TOPIC = "delete_object";

    /**
     * 副本备份消息主题
     */
    public static final String REPLICA_TOPIC = "copy_replica";

    /**
     * 消费间隔
     */
    public static final long CONSUME_INTERVAL = 10 * 1000;
}
