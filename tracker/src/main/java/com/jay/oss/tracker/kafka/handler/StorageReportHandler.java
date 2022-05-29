package com.jay.oss.tracker.kafka.handler;

import com.jay.oss.common.kafka.RecordHandler;
import com.jay.oss.common.util.StringUtil;
import com.jay.oss.tracker.track.ObjectTracker;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * <p>
 *  Storage汇报消息处理器
 * </p>
 *
 * @author Jay
 * @date 2022/05/17 15:58
 */
public class StorageReportHandler implements RecordHandler {

    private final ObjectTracker objectTracker;

    public StorageReportHandler(ObjectTracker objectTracker) {
        this.objectTracker = objectTracker;
    }

    @Override
    public void handle(Iterable<ConsumerRecord<String, String>> records, ConsumerGroupMetadata groupMeta) {
        for (ConsumerRecord<String, String> record : records) {
            String address = record.key();
            String[] objectIds = record.value().split(";");
            if(objectIds.length > 0 && !StringUtil.isNullOrEmpty(objectIds[0])){
                // 从objects中过滤出被删除的对象，并向所有Storage节点发送删除消息
                List<Long> ids = Arrays.stream(objectIds)
                        .map(Long::parseLong).collect(Collectors.toList());
                objectTracker.addObjectReplicasLocation(address, ids);
            }
        }
    }
}
