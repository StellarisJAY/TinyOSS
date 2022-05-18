package com.jay.oss.storage.task;

import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.constant.OssConstants;
import com.jay.oss.common.kafka.RecordProducer;
import com.jay.oss.common.prometheus.GaugeManager;
import com.jay.oss.common.registry.Registry;
import com.jay.oss.common.registry.StorageNodeInfo;
import com.jay.oss.common.util.NodeInfoCollector;
import com.jay.oss.storage.fs.ObjectIndexManager;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/05/17 14:13
 */
@Slf4j
public class StorageNodeHeartBeatTask implements Runnable{
    private final ObjectIndexManager objectIndexManager;
    private final Registry registry;
    private final StorageTaskManager storageTaskManager;
    private final int port;
    private final RecordProducer recordProducer;

    public StorageNodeHeartBeatTask(ObjectIndexManager objectIndexManager, Registry registry, StorageTaskManager storageTaskManager, int port, RecordProducer recordProducer) {
        this.objectIndexManager = objectIndexManager;
        this.registry = registry;
        this.storageTaskManager = storageTaskManager;
        this.port = port;
        this.recordProducer = recordProducer;
    }

    @Override
    public void run() {
        try{
            StorageNodeInfo nodeInfo = NodeInfoCollector.getStorageNodeInfo(port);
            List<Long> storedObjects = objectIndexManager.listObjectIds();
            if(OssConfigs.enableTrackerRegistry() && OssConfigs.enableTrackerMessaging()){
                // 心跳消息模式下，使用心跳将对象列表发送出去
                nodeInfo.setStoredObjects(storedObjects);
                Optional.ofNullable(registry.trackerHeartBeat(nodeInfo))
                        .ifPresent(response->{
                            storageTaskManager.addReplicaTasks(response.getReplicaTasks());
                            storageTaskManager.addDeleteTask(response.getDeleteTasks());
                        });
            }else{
                // 使用zookeeper更新storage节点状态
                registry.update(nodeInfo);
                // 使用消息队列汇报当前storage节点保存的对象ID列表
                StringJoiner joiner = new StringJoiner(";");
                storedObjects.forEach(id->joiner.add(Long.toString(id)));
                recordProducer.send(OssConstants.REPORT_TOPIC, nodeInfo.getUrl(), joiner.toString());
            }
            // 更新存储容量监控数据
            GaugeManager.getGauge("storage_used").set(nodeInfo.getUsedSpace());
            GaugeManager.getGauge("storage_free").set(nodeInfo.getSpace());
        }catch (Exception e){
            log.warn("update storage node info error ", e);
        }
    }
}
