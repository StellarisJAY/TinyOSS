package com.jay.oss.tracker.replica;

import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.constant.OssConstants;
import com.jay.oss.common.entity.object.ObjectMeta;
import com.jay.oss.common.entity.task.DeleteTask;
import com.jay.oss.common.entity.task.ReplicaTask;
import com.jay.oss.common.kafka.RecordProducer;
import com.jay.oss.common.registry.StorageNodeInfo;
import com.jay.oss.common.util.Scheduler;
import com.jay.oss.tracker.registry.StorageNodeRegistry;
import com.jay.oss.tracker.task.StorageTaskManager;
import com.jay.oss.tracker.track.ObjectTracker;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  副本平衡
 *  定期检测每个文件的副本数量
 *  如果副本数量不满足配置要求则重新分配存储节点
 * </p>
 *
 * @author Jay
 * @date 2022/05/23 14:08
 */
@Slf4j
public class ReplicaBalancer {
    /**
     * 从上传一个副本成功到检测副本不平衡的间隔时间，避免正在副本复制时平衡副本
     */
    public static final long POST_BALANCE_TIME = 60 * 1000;
    private final ObjectTracker objectTracker;
    private final StorageTaskManager taskManager;
    private final StorageNodeRegistry storageNodeRegistry;
    private final RecordProducer recordProducer;

    public ReplicaBalancer(ObjectTracker objectTracker, StorageTaskManager taskManager, StorageNodeRegistry storageNodeRegistry, RecordProducer recordProducer) {
        this.objectTracker = objectTracker;
        this.taskManager = taskManager;
        this.storageNodeRegistry = storageNodeRegistry;
        this.recordProducer = recordProducer;
    }

    public void init() {
        /*
            副本再平衡任务
            1. 检测哪些文件的副本数量小于要求的副本数量，对这些文件重新分配storage，然后向storage发布副本复制任务
            2. 检测哪些文件的副本数量大于要求的副本数量，向多余的storage发布删除任务
         */
        Runnable task = ()->{
            // 列出所有没被删除的对象
            Set<Long> objectIds = objectTracker.listObjectIds();
            int requiredReplicas = OssConfigs.replicaCount();
            for (Long id : objectIds) {
                // 获取该对象的所有副本位置
                Set<String> locations = objectTracker.getObjectReplicaLocations(id);
                if (locations == null || locations.isEmpty()){
                    log.warn("No Replicas found for object: {}", id);
                    continue;
                }
                if(locations.size() == requiredReplicas){
                    continue;
                }
                // 小于要求的副本数量
                if (locations.size() < requiredReplicas){
                    ObjectMeta meta = objectTracker.getObjectMetaById(id);
                    // 判断是否处于上传时的副本复制阶段
                    if (meta == null || System.currentTimeMillis() - meta.getCreateTime() < POST_BALANCE_TIME) {
                        continue;
                    }
                    int replicaCount = requiredReplicas - locations.size();
                    try{
                        // 分配存储服务器
                        List<StorageNodeInfo> targetStorages = storageNodeRegistry.balanceReplica(meta.getSize(), replicaCount, locations);
                        String srcLocation = (String)locations.toArray()[0];
                        targetStorages.forEach(node-> sendReplicaTask(node.getUrl(), srcLocation, id, meta.getSize()));
                    }catch (Exception e) {
                        log.warn("Re-balance replica task failed for: {}, no enough storage nodes", id);
                    }
                }else{
                    // 向多余的storages发送删除任务
                    locations.stream()
                            .limit(locations.size() - requiredReplicas)
                            .forEach(location->sendDeleteTask(location, id));
                }
            }
        };
        // 开启定时检测任务
        Scheduler.scheduleAtFixedRate(task, OssConfigs.balanceReplicaInterval(), OssConfigs.balanceReplicaInterval(), TimeUnit.MILLISECONDS);
    }

    /**
     * 发布副本复制任务
     * @param location 目标storage地址
     * @param srcLocation 副本位置
     * @param objectId 对象ID
     */
    private void sendReplicaTask(String location, String srcLocation, long objectId, int size) {
        String topicSuffix = "_" + location.replace(":", "_");
        if(OssConfigs.enableTrackerMessaging()) {
            // 发布副本复制任务
            ReplicaTask replicaTask = new ReplicaTask(0L, objectId, size, srcLocation);
            taskManager.addReplicaTask(location, replicaTask);
        }else{
            recordProducer.send(OssConstants.REPLICA_TOPIC + topicSuffix, Long.toString(objectId), srcLocation);
        }
    }

    /**
     * 发布删除多余副本任务
     * @param location storage地址
     * @param objectId 对象ID
     */
    private void sendDeleteTask(String location, long objectId) {
        String topicSuffix = "_" + location.replace(":", "_");
        if(OssConfigs.enableTrackerMessaging()) {
            // 创建删除任务
            DeleteTask deleteTask = new DeleteTask(0, objectId);
            taskManager.addDeleteTask(location, deleteTask);
        }else{
            recordProducer.send(OssConstants.DELETE_OBJECT_TOPIC + topicSuffix, Long.toString(objectId), Long.toString(objectId));
        }
    }
}
