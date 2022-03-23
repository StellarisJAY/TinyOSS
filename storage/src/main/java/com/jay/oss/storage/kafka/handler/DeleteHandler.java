package com.jay.oss.storage.kafka.handler;

import com.jay.oss.common.edit.EditLog;
import com.jay.oss.common.edit.EditLogManager;
import com.jay.oss.common.edit.EditOperation;
import com.jay.oss.common.entity.FileMetaWithChunkInfo;
import com.jay.oss.common.kafka.RecordHandler;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.common.util.StringUtil;
import com.jay.oss.storage.fs.Chunk;
import com.jay.oss.storage.fs.ChunkManager;
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

            if(meta != null && !meta.isMarkRemoved() && !meta.isRemoved()){
                meta.setMarkRemoved(true);
                // 找到存储object的块
                Chunk chunk = chunkManager.getChunkById(meta.getChunkId());
                if(chunk != null){
                    // 惰性删除，如果返回列表不为空，表示完成了一次压缩，返回的列表是压缩后offset改变和完全删除的对象
                    List<FileMetaWithChunkInfo> changed = chunk.deleteObject(meta);
                    if(changed != null){
                        // 重写EditLog
                        appendChangedEditLog(changed);
                        log.info("Object Deleted and Chunk compact done, object: {}, chunk: {}", objectKey, chunk.getId());
                    }else{
                        appendMarkRemovedEditLog(objectKey);
                        log.info("Object Marked deleted, object: {}", objectKey);
                    }
                }
                else{
                    log.warn("Can't delete object, chunk not found, object: {}, chunk: {}", objectKey, meta.getChunkId());
                }
            }
        }
    }


    /**
     * 添加标记删除editLog
     * @param objectKey objectKey
     */
    private void appendMarkRemovedEditLog(String objectKey){
        byte[] keyBytes = StringUtil.getBytes(objectKey);
        EditLog editLog = new EditLog(EditOperation.MARK_DELETE, keyBytes);
        editLogManager.append(editLog);
    }

    /**
     * 添加chunk压缩后的object偏移量改变editLog
     * @param metas object meta集合
     */
    private void appendChangedEditLog(List<FileMetaWithChunkInfo> metas){
        if(metas != null && !metas.isEmpty()){
            for (FileMetaWithChunkInfo meta : metas) {
                if(meta.isRemoved()){
                    EditLog editLog = new EditLog(EditOperation.DELETE, StringUtil.getBytes(meta.getKey()));
                    editLogManager.append(editLog);
                }
                else{
                    byte[] serialized = SerializeUtil.serialize(meta, FileMetaWithChunkInfo.class);
                    EditLog editLog = new EditLog(EditOperation.ADD, serialized);
                    editLogManager.append(editLog);
                }
            }
        }
    }
}
