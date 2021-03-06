package com.jay.oss.storage.kafka.handler;

import com.jay.dove.DoveClient;
import com.jay.dove.transport.Url;
import com.jay.dove.transport.command.CommandCode;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.entity.request.GetObjectRequest;
import com.jay.oss.common.kafka.RecordHandler;
import com.jay.oss.common.remoting.TinyOssCommand;
import com.jay.oss.common.remoting.TinyOssProtocol;
import com.jay.oss.storage.fs.Block;
import com.jay.oss.storage.fs.BlockManager;
import com.jay.oss.storage.fs.ObjectIndex;
import com.jay.oss.storage.fs.ObjectIndexManager;
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
    private final ObjectIndexManager objectIndexManager;
    private final BlockManager blockManager;

    public ReplicaHandler(DoveClient client, ObjectIndexManager objectIndexManager, BlockManager blockManager) {
        try{
            this.blockManager = blockManager;
            this.client = client;
            this.objectIndexManager = objectIndexManager;
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public void handle(Iterable<ConsumerRecord<String, String>> records, ConsumerGroupMetadata groupMeta) {
        // 遍历消息列表
        for (ConsumerRecord<String, String> record : records) {
            long objectId = Long.parseLong(record.key());
            String sourceUrl = record.value();
            getObject(objectId, sourceUrl);
        }
    }
    /**
     * 从目标服务器读取对象
     * @param objectId 对象Key
     * @param url 目标服务器地址
     */
    private void getObject(long objectId, String url){
        try{
            GetObjectRequest request = new GetObjectRequest(objectId, 0, -1);
            RemotingCommand command = client.getCommandFactory()
                    .createRequest(request, TinyOssProtocol.DOWNLOAD_FULL, GetObjectRequest.class);
            long start = System.currentTimeMillis();
            // 向目标主机发送GetObject请求
            TinyOssCommand response = (TinyOssCommand)client.sendSync(Url.parseString(url), command, null);
            CommandCode code = response.getCommandCode();
            if(TinyOssProtocol.DOWNLOAD_RESPONSE.equals(code)){
                // 保存对象数据
                saveObject(objectId, response.getData());
            }
            else{
                log.error("Object replica not found, object:{}, src:{}", objectId, url);
            }
        }catch (Exception e){
            log.error("Get Object Failed, object: {}, location: {}", objectId, url);
        }
    }

    /**
     * 保存对象数据
     * @param objectId object ID
     * @param data {@link ByteBuf} object数据
     */
    private void saveObject(long objectId, ByteBuf data) {
        objectIndexManager.computeIfAbsent(objectId, (id)->{
            int size = data.readableBytes();
            Block block = blockManager.getBlockBySize(size);
            ObjectIndex index = block.write(objectId, data, size);
            blockManager.offerBlock(block);
            data.release();
            return index;
        });
    }
}
