package com.jay.oss.tracker.kafka.handler;

import com.jay.oss.common.constant.OssConstants;
import com.jay.oss.common.kafka.RecordHandler;
import com.jay.oss.common.kafka.RecordProducer;
import com.jay.oss.tracker.track.ObjectTracker;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;

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
            String storageUrl = record.value().trim();
            long objectId = Long.parseLong(record.key());
            // 判断object是否被删除
            if(objectTracker.isObjectDeleted(objectId)){
                // 通知storage节点删除object
                producer.send(OssConstants.DELETE_OBJECT_TOPIC, record.key(), record.key());
            }else{
                // 记录object位置
                objectTracker.addObjectReplicaLocation(objectId, storageUrl);
            }
        }
    }
}
