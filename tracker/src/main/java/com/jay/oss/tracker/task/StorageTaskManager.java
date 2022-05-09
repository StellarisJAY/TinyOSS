package com.jay.oss.tracker.task;

import com.jay.oss.common.entity.task.DeleteTask;
import com.jay.oss.common.entity.task.ReplicaTask;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/05/09 13:24
 */
public class StorageTaskManager {
    private final ConcurrentHashMap<String, Queue<ReplicaTask>> replicaTaskQueues = new ConcurrentHashMap<>(64);
    private final ConcurrentHashMap<String, Queue<DeleteTask>> deleteTaskQueues = new ConcurrentHashMap<>(64);

    public void addReplicaTask(String storageNode, ReplicaTask task){
        replicaTaskQueues.computeIfAbsent(storageNode, key->new LinkedBlockingQueue<>());
        replicaTaskQueues.get(storageNode).offer(task);
    }

    public void addDeleteTask(String storageNode, DeleteTask task){
        deleteTaskQueues.computeIfAbsent(storageNode, key->new LinkedBlockingQueue<>());
        deleteTaskQueues.get(storageNode).offer(task);
    }

    public List<ReplicaTask> pollReplicaTasks(String storageNode, int limit){
        List<ReplicaTask> tasks = new LinkedList<>();
        Queue<ReplicaTask> queue = replicaTaskQueues.get(storageNode);
        if(queue == null || queue.isEmpty()){
            return tasks;
        }
        for(int i = 0; i < limit; i ++){
            ReplicaTask task = queue.poll();
            if(task == null){
                break;
            }
            tasks.add(task);
        }
        return tasks;
    }

    public List<DeleteTask> pollDeleteTask(String storageNode, int limit){
        List<DeleteTask> tasks = new LinkedList<>();
        Queue<DeleteTask> queue = deleteTaskQueues.get(storageNode);
        if(queue == null || queue.isEmpty()){
            return tasks;
        }
        for(int i = 0; i < limit; i ++){
            DeleteTask task = queue.poll();
            if(task == null){
                break;
            }
            tasks.add(task);
        }
        return tasks;
    }
}
