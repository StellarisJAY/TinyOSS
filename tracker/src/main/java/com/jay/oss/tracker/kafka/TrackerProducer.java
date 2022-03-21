package com.jay.oss.tracker.kafka;

import com.jay.oss.common.config.OssConfigs;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * <p>
 *  Tracker端消息生产者
 * </p>
 *
 * @author Jay
 * @date 2022/03/21 11:59
 */
@Slf4j
public class TrackerProducer {
    private final KafkaProducer<String, String> producer;

    public TrackerProducer(){
        this.producer = new KafkaProducer<>(getProperties(), new StringSerializer(), new StringSerializer());
    }

    private Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, OssConfigs.kafkaServers());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, OssConfigs.kafkaAcks());
        return properties;
    }

    /**
     * 发送消息到目标主题
     * @param topic 主题
     * @param key key
     * @param value value
     * @return {@link Future<RecordMetadata>} 异步结果
     */
    public final Future<RecordMetadata> send(String topic, String key, String value){
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        return producer.send(record, (metadata, exception) -> {
            if(exception != null){
                // 消息发送错误，等待重发
                log.error("Producer send record failed, topic: {}, key: {}", topic, key, exception);
            }
            else{
                log.info("Producer send record success, topic: {}, key: {}", topic, key);
            }
        });
    }

}
