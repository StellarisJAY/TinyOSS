package com.jay.oss.storage.kafka.handler;

import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.edit.EditLog;
import com.jay.oss.common.edit.EditLogManager;
import com.jay.oss.common.edit.EditOperation;
import com.jay.oss.common.entity.FileMetaWithChunkInfo;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.storage.fs.Chunk;
import com.jay.oss.storage.fs.ChunkManager;
import com.jay.oss.common.kafka.RecordHandler;
import com.jay.oss.storage.meta.MetaManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

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
    private final ChunkManager chunkManager;
    private final EditLogManager editLogManager;

    public DeleteHandler(MetaManager metaManager, ChunkManager chunkManager, EditLogManager editLogManager) {
        this.metaManager = metaManager;
        this.chunkManager = chunkManager;
        this.editLogManager = editLogManager;
    }

    @Override
    public void handle(Iterable<ConsumerRecord<String, String>> records, ConsumerGroupMetadata groupMetadata) {
        for (ConsumerRecord<String, String> record : records) {
            String objectKey = record.value();
            FileMetaWithChunkInfo meta = metaManager.getMeta(objectKey);

            if(meta != null && !meta.isRemoved()){
                meta.setRemoved(true);
                // 找到存储object的块
                Chunk chunk = chunkManager.getChunkById(meta.getChunkId());
                if(chunk != null){
                    // 惰性删除，如果返回列表不为空，表示完成了一次压缩，有object的位置改变需要重写EditLog
                    List<FileMetaWithChunkInfo> offsetChanged = chunk.deleteObject(meta);
                    appendOffsetChangeLog(offsetChanged);
                }
                appendEditLog(objectKey);
                log.info("Deleted object: {}, meta: {}", objectKey, meta);
            }
        }
    }

    /**
     * 添加object删除editLog
     * @param key objectKey
     */
    private void appendEditLog(String key){
        byte[] keyBytes = key.getBytes(OssConfigs.DEFAULT_CHARSET);
        EditLog editLog = new EditLog(EditOperation.DELETE, keyBytes);
        editLogManager.append(editLog);
    }

    /**
     * 添加chunk压缩后的object偏移量改变editLog
     * @param metas object meta集合
     */
    private void appendOffsetChangeLog(List<FileMetaWithChunkInfo> metas){
        if(metas != null && !metas.isEmpty()){
            for (FileMetaWithChunkInfo meta : metas) {
                byte[] serialized = SerializeUtil.serialize(meta, FileMetaWithChunkInfo.class);
                EditLog editLog = new EditLog(EditOperation.ADD, serialized);
                editLogManager.append(editLog);
            }
        }
    }
}
