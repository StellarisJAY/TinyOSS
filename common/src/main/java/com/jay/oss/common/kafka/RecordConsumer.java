package com.jay.oss.common.kafka;

import com.jay.dove.common.AbstractLifeCycle;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.constant.OssConstants;
import com.jay.oss.common.util.NodeInfoCollector;
import com.jay.oss.common.util.ThreadPoolUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * <p>
 *  消息消费者
 * </p>
 *
 * @author Jay
 * @date 2022/03/21 13:33
 */
@Slf4j
public class RecordConsumer extends AbstractLifeCycle {
    private final KafkaConsumer<String, String> consumer;
    /**
     * 消息处理器Map
     */
    private final Map<String, RecordHandler> handlers = new HashMap<>();
    /**
     * 消息处理器线程池
     */
    private final ExecutorService handlerExecutor = ThreadPoolUtil.newThreadPool(2, 2, "message-handler-", new ThreadPoolExecutor.CallerRunsPolicy());

    private Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, OssConfigs.kafkaServers());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, NodeInfoCollector.getAddress());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return properties;
    }

    public RecordConsumer(){
        this.consumer = new KafkaConsumer<>(getProperties(), new StringDeserializer(), new StringDeserializer());
    }

    @Override
    public final void startup(){
        super.startup();
        // 订阅删除消息 和 备份消息
        this.consumer.subscribe(handlers.keySet());
        // 开启消费循环
        handlerExecutor.submit(this::consume);
        log.info("Kafka Consumer started, kafka host: {}, subscribed topics: {}", OssConfigs.kafkaServers(), handlers.keySet());
    }

    /**
     * 消息poll循环
     */
    private void consume(){
        while(isStarted()){
            ConsumerRecords<String, String> pollResult = consumer.poll(Duration.ofMillis(OssConstants.CONSUME_INTERVAL));
            ConsumerGroupMetadata groupMetadata = consumer.groupMetadata();
            for (Map.Entry<String, RecordHandler> entry : handlers.entrySet()) {
                RecordHandler handler;
                if((handler = entry.getValue()) != null){
                    String topic = entry.getKey();
                    Iterable<ConsumerRecord<String, String>> records = pollResult.records(topic);
                    handlerExecutor.execute(()->handler.handle(records, groupMetadata));
                }
            }
            consumer.commitAsync();
        }
    }

    /**
     * 订阅主题，注册消息处理器
     * @param topic 消息主题
     * @param handler {@link RecordHandler} 消息处理器
     */
    public final void subscribeTopic(String topic, RecordHandler handler){
        handlers.put(topic, handler);
    }


    @Override
    public void shutdown() {
        super.shutdown();
        consumer.close();
    }
}
