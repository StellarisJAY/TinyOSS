package com.jay.oss.tracker.replica;

import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.entity.object.ObjectMeta;
import com.jay.oss.common.entity.task.ReplicaTask;
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
    public static final long POST_BALANCE_TIME = 10 * 60 * 1000;

    private final ObjectTracker objectTracker;
    private final StorageTaskManager taskManager;
    private final StorageNodeRegistry storageNodeRegistry;

    public ReplicaBalancer(ObjectTracker objectTracker, StorageTaskManager taskManager, StorageNodeRegistry storageNodeRegistry) {
        this.objectTracker = objectTracker;
        this.taskManager = taskManager;
        this.storageNodeRegistry = storageNodeRegistry;
    }

    public void init() {
        Runnable task = ()->{
            Set<Long> objectIds = objectTracker.listObjectIds();
            for (Long id : objectIds) {
                Set<String> locations = objectTracker.getObjectReplicaLocations(id);
                if (locations == null || locations.isEmpty()){
                    continue;
                }
                // 判断是否满足配置要求的副本数量
                if (locations.size() < OssConfigs.replicaCount()){
                    ObjectMeta meta = objectTracker.getObjectMetaById(id);
                    // 判断是否处于上传时的副本复制阶段
                    if (meta == null || System.currentTimeMillis() - meta.getCreateTime() < POST_BALANCE_TIME) {
                        continue;
                    }
                    int replicaCount = OssConfigs.replicaCount() - locations.size();
                    try{
                        // 分配存储服务器
                        List<StorageNodeInfo> targetStorages = storageNodeRegistry.balanceReplica(meta.getSize(), replicaCount, locations);
                        String srcLocation = (String)locations.toArray()[0];
                        // 发布副本复制任务
                        ReplicaTask replicaTask = new ReplicaTask(0L, id, srcLocation);
                        targetStorages.forEach(node-> taskManager.addReplicaTask(node.getUrl(), replicaTask));
                    }catch (Exception e) {
                        log.warn("Replica balance error ", e);
                    }
                }
            }
        };
        Scheduler.scheduleAtFixedRate(task, OssConfigs.balanceReplicaInterval(), OssConfigs.balanceReplicaInterval(), TimeUnit.MILLISECONDS);
    }
}
