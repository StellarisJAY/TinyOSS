package com.jay.oss.storage.kafka.handler;

import com.jay.dove.DoveClient;
import com.jay.dove.transport.Url;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.edit.EditLog;
import com.jay.oss.common.edit.EditLogManager;
import com.jay.oss.common.edit.EditOperation;
import com.jay.oss.common.entity.DownloadRequest;
import com.jay.oss.common.entity.FileMetaWithChunkInfo;
import com.jay.oss.common.kafka.RecordHandler;
import com.jay.oss.common.remoting.FastOssCommand;
import com.jay.oss.common.remoting.FastOssProtocol;
import com.jay.oss.common.util.NodeInfoCollector;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.storage.fs.Chunk;
import com.jay.oss.storage.fs.ChunkManager;
import com.jay.oss.storage.meta.MetaManager;
import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * <p>
 *  异步备份消息处理器
 * </p>
 *
 * @author Jay
 * @date 2022/03/21 13:55
 */
@Slf4j
public class ReplicaHandler implements RecordHandler {

    private final DoveClient client;
    private final ChunkManager chunkManager;
    private final EditLogManager editLogManager;
    private final MetaManager metaManager;

    private final String nodeUrl;

    public ReplicaHandler(DoveClient client, ChunkManager chunkManager, EditLogManager editLogManager,
                          MetaManager metaManager) {
        try{
            this.client = client;
            this.chunkManager = chunkManager;
            this.editLogManager = editLogManager;
            this.metaManager = metaManager;
            this.nodeUrl = NodeInfoCollector.getAddress();
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public void handle(Iterable<ConsumerRecord<String, String>> records, ConsumerGroupMetadata groupMeta) {
        for (ConsumerRecord<String, String> record : records) {
            String objectKey = record.key();
            String value = record.value();
            String[] split = value.split(";");
            if(split.length == 2 && split[1].equals(nodeUrl)){
                getObject(objectKey, split[0]);
            }
        }
    }

    /**
     * 从目标服务器读取对象
     * @param objectKey 对象Key
     * @param url 目标服务器地址
     */
    private void getObject(String objectKey, String url){
        try{
            DownloadRequest request = new DownloadRequest(objectKey, true, 0, 0);
            RemotingCommand command = client.getCommandFactory()
                    .createRequest(request, FastOssProtocol.DOWNLOAD_FULL, DownloadRequest.class);
            long start = System.currentTimeMillis();
            // 向目标主机发送GetObject请求
            FastOssCommand response = (FastOssCommand)client.sendSync(Url.parseString(url), command, null);
            CommandCode code = response.getCommandCode();
            if(FastOssProtocol.DOWNLOAD_RESPONSE.equals(code)){
                saveObject(objectKey, response.getData());
                log.info("Object backup task done, object: {}, src: {}, time used: {}ms", objectKey, url, (System.currentTimeMillis() - start));
            }
            else{
                log.error("Object replica not found, object:{}, src:{}", objectKey, url);
            }
        }catch (Exception e){
            log.error("Get Object Failed, object: {}, location: {}", objectKey, url);
        }
    }

    /**
     * 保存对象数据
     * @param objectKey object key
     * @param data {@link ByteBuf} object数据
     */
    private void saveObject(String objectKey, ByteBuf data) {
        int size = data.readableBytes();
        // 获取chunk
        Chunk chunk = chunkManager.getChunkBySize(size);
        try{
            // 计算offset
            int offset = chunk.getAndAddSize(size);
            // 写入数据
            chunk.writeHead(data, offset);
            FileMetaWithChunkInfo meta = FileMetaWithChunkInfo.builder()
                    .chunkId(chunk.getId()).removed(false)
                    .offset(offset).size(size)
                    .key(objectKey)
                    .build();
            // 缓存记录object位置
            metaManager.saveMeta(meta);
            // 记录editLog
            appendEditLog(meta);
        }catch (Exception e){
            log.error("Save Object Replica Failed object: {}", objectKey, e);
        }finally {
            chunkManager.offerChunk(chunk);
        }
    }

    /**
     * EditLog记录object位置
     * @param meta {@link FileMetaWithChunkInfo}
     */
    private void appendEditLog(FileMetaWithChunkInfo meta){
        byte[] serialized = SerializeUtil.serialize(meta, FileMetaWithChunkInfo.class);
        // 生成编辑日志
        EditLog editLog = new EditLog(EditOperation.ADD, serialized);
        editLogManager.append(editLog);
    }
}
