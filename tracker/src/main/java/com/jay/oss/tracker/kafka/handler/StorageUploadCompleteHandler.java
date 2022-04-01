package com.jay.oss.tracker.kafka.handler;

import com.jay.oss.common.constant.OssConstants;
import com.jay.oss.common.kafka.RecordHandler;
import com.jay.oss.common.kafka.RecordProducer;
import com.jay.oss.common.util.StringUtil;
import com.jay.oss.tracker.track.ObjectTracker;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * <p>
 *  上传完成消息处理器
 * </p>
 *
 * @author Jay
 * @date 2022/03/21 13:43
 */
@Slf4j
public class StorageUploadCompleteHandler implements RecordHandler {
    private final RecordProducer producer;
    private final ObjectTracker objectTracker;

    public StorageUploadCompleteHandler(RecordProducer producer, ObjectTracker objectTracker) {
        this.producer = producer;
        this.objectTracker = objectTracker;
    }

    @Override
    public void handle(Iterable<ConsumerRecord<String, String>> records, ConsumerGroupMetadata groupMetadata) {
        for (ConsumerRecord<String, String> record : records) {
            String objectKey = record.key();
            String storageUrl = record.value().trim();
            String urls = objectTracker.locateObject(objectKey);
            if(!StringUtil.isNullOrEmpty(urls)){
                String[] storages = urls.split(";");
                // 排除已上传成功的storage
                List<String> backupUrls = Arrays.stream(storages)
                        .filter(url -> !storageUrl.equalsIgnoreCase(url))
                        .collect(Collectors.toList());
                for (String backupUrl : backupUrls) {
                    // 发送备份消息到指定的备份机器
                    producer.send(OssConstants.REPLICA_TOPIC, objectKey, storageUrl + ";" + backupUrl);
                }
            }
        }
    }
}
