package com.jay.oss.storage.kafka.handler;

import com.jay.oss.common.kafka.RecordHandler;
import com.jay.oss.storage.fs.Block;
import com.jay.oss.storage.fs.BlockManager;
import com.jay.oss.storage.fs.ObjectIndex;
import com.jay.oss.storage.meta.MetaManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * <p>
 *  删除消息处理器
 * </p>
 *
 * @author Jay
 * @date 2022/03/21 11:45
 */
@Slf4j
public class DeleteHandler implements RecordHandler {

    private final MetaManager metaManager;
    private final BlockManager blockManager;

    public DeleteHandler(MetaManager metaManager, BlockManager blockManager) {
        this.metaManager = metaManager;
        this.blockManager = blockManager;
    }

    @Override
    public void handle(Iterable<ConsumerRecord<String, String>> records, ConsumerGroupMetadata groupMetadata) {
        for (ConsumerRecord<String, String> record : records) {
            long objectId = Long.parseLong(record.key());
            ObjectIndex index = metaManager.getObjectIndex(objectId);
            Block block;
            if(index != null && (block = blockManager.getBlockById(index.getBlockId())) != null){
                if(block.delete(objectId, index.getOffset())){
                    metaManager.deleteIndex(objectId);
                }
            }
        }
    }
}
