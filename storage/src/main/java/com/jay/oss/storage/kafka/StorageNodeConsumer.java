package com.jay.oss.storage.kafka;

import com.jay.dove.common.AbstractLifeCycle;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.constant.OssConstants;
import com.jay.oss.common.util.ThreadPoolUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;

/**
 * <p>
 *  Storage端消息消费者
 * </p>
 *
 * @author Jay
 * @date 2022/03/21 11:07
 */
@Slf4j
public class StorageNodeConsumer extends AbstractLifeCycle {
    private final KafkaConsumer<String, String> consumer;
    /**
     * 消息处理器Map
     */
    private final Map<String, RecordHandler> handlers = new HashMap<>();
    /**
     * 消息处理器线程池
     */
    private final ExecutorService handlerExecutor = ThreadPoolUtil.newIoThreadPool("message-handler-");

    private Properties getProperties() throws UnknownHostException {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, OssConfigs.kafkaServers());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, Inet4Address.getLocalHost().getHostAddress() + ":" + OssConfigs.port());
        return properties;
    }

    public StorageNodeConsumer() throws Exception{
        this.consumer = new KafkaConsumer<>(getProperties(), new StringDeserializer(), new StringDeserializer());
    }

    @Override
    public final void startup(){
        super.startup();
        // 订阅删除消息 和 备份消息
        this.consumer.subscribe(Collections.singletonList(OssConstants.DELETE_OBJECT_TOPIC));
        // 开启消费循环
        handlerExecutor.submit(this::consume);
        log.info("Storage Kafka Consumer started");
    }

    /**
     * 消息poll循环
     */
    private void consume(){
        while(isStarted()){
            ConsumerRecords<String, String> pollResult = consumer.poll(Duration.ofMillis(OssConstants.CONSUME_INTERVAL));
            RecordHandler deleteHandler, replicaHandler;
            if((deleteHandler = handlers.get(OssConstants.DELETE_OBJECT_TOPIC)) != null){
                handlerExecutor.submit(()->deleteHandler.handle(pollResult.records(OssConstants.DELETE_OBJECT_TOPIC)));
            }
            if((replicaHandler = handlers.get(OssConstants.REPLICA_TOPIC)) != null){
                handlerExecutor.submit(()->replicaHandler.handle(pollResult.records(OssConstants.REPLICA_TOPIC)));
            }
        }
    }

    /**
     * 注册消息处理器
     * @param topic 消息主题
     * @param handler {@link RecordHandler} 消息处理器
     */
    public final void registerHandler(String topic, RecordHandler handler){
        handlers.put(topic, handler);
    }


    @Override
    public void shutdown() {
        super.shutdown();
        consumer.close();
    }
}
