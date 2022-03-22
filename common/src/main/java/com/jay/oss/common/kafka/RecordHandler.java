package com.jay.oss.common.kafka;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * <p>
 *  消息处理器接口
 * </p>
 *
 * @author Jay
 * @date 2022/03/21 11:11
 */
public interface RecordHandler {

    /**
     * 批量处理消息
     * @param records {@link ConsumerRecord} 消息列表
     * @param groupMeta {@link ConsumerGroupMetadata} 消费者组信息
     */
    void handle(Iterable<ConsumerRecord<String, String>> records, ConsumerGroupMetadata groupMeta);
}
