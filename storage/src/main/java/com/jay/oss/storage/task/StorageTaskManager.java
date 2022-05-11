package com.jay.oss.storage.task;

import com.jay.dove.DoveClient;
import com.jay.dove.transport.Url;
import com.jay.dove.transport.command.RemotingCommand;
import com.jay.oss.common.entity.request.GetObjectRequest;
import com.jay.oss.common.entity.task.DeleteTask;
import com.jay.oss.common.entity.task.ReplicaTask;
import com.jay.oss.common.remoting.TinyOssCommand;
import com.jay.oss.common.remoting.TinyOssProtocol;
import com.jay.oss.common.util.Scheduler;
import com.jay.oss.storage.fs.Block;
import com.jay.oss.storage.fs.BlockManager;
import com.jay.oss.storage.fs.ObjectIndex;
import com.jay.oss.storage.fs.ObjectIndexManager;
import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  StorageNode 任务处理器
 *  由Tracker服务通过心跳发送给StorageNode任务，StorageNode将任务保存在任务队列中
 *  后台线程定时从队列消费任务
 * </p>
 *
 * @author Jay
 * @date 2022/05/09 14:44
 */
@Slf4j
public class StorageTaskManager {
    private final Queue<ReplicaTask> replicaTasks = new LinkedBlockingQueue<>();
    private final Queue<DeleteTask> deleteTasks = new LinkedBlockingQueue<>();

    private final DoveClient storageClient;
    private final BlockManager blockManager;
    private final ObjectIndexManager objectIndexManager;

    private static final int REPLICA_COPY_COUNT = 10;

    public StorageTaskManager(DoveClient storageClient, BlockManager blockManager, ObjectIndexManager objectIndexManager) {
        this.storageClient = storageClient;
        this.blockManager = blockManager;
        this.objectIndexManager = objectIndexManager;
        Scheduler.scheduleAtFixedRate(new ReplicaTaskHandler(), 1000, 1000, TimeUnit.MILLISECONDS);
        Scheduler.scheduleAtFixedRate(new DeleteTaskHandler(), 500, 500, TimeUnit.MILLISECONDS);
    }

    /**
     * 添加副本复制任务
     * @param tasks {@link ReplicaTask}
     */
    public void addReplicaTasks(List<ReplicaTask> tasks){
        if(tasks != null && !tasks.isEmpty()){
            replicaTasks.addAll(tasks);
        }
    }

    /**
     * 添加删除副本任务
     * @param tasks {@link DeleteTask}
     */
    public void addDeleteTask(List<DeleteTask> tasks){
        if(tasks != null && !tasks.isEmpty()){
            deleteTasks.addAll(tasks);
        }
    }

    /**
     * 副本复制任务处理器
     * 从任务队列消费一个副本复制任务，然后从目标地址下载副本并保存
     */
    class ReplicaTaskHandler implements Runnable{
        @Override
        public void run() {
            for(int i = 0 ; i < REPLICA_COPY_COUNT; i++){
                ReplicaTask task = replicaTasks.poll();
                if(task == null){
                    break;
                }
                try{
                    Url url = Url.parseString(task.getStorageUrl());
                    GetObjectRequest request = new GetObjectRequest(task.getObjectId(), 0, -1);
                    RemotingCommand command = storageClient.getCommandFactory().createRequest(request, TinyOssProtocol.DOWNLOAD_FULL, GetObjectRequest.class);
                    TinyOssCommand response = (TinyOssCommand)storageClient.sendSync(url, command, null);
                    saveObject(task.getObjectId(), response.getData());
                }catch (Exception e){
                    log.warn("Failed to copy replica from : {}", task.getStorageUrl());
                    replicaTasks.offer(task);
                }
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

    /**
     * 删除任务处理器
     * 从删除任务队列消费一个任务，然后在文件系统标记删除
     */
    class DeleteTaskHandler implements Runnable{
        @Override
        public void run() {
            DeleteTask task = deleteTasks.poll();
            if(task != null){
                long startTime = System.currentTimeMillis();
                long objectId = task.getObjectId();
                ObjectIndex index = objectIndexManager.getObjectIndex(objectId);
                Block block;
                if(index != null && (block = blockManager.getBlockById(index.getBlockId())) != null){
                    if(block.delete(objectId, index.getOffset())){
                        index.setRemoved(true);
                        log.info("Object {} Marked deleted, time used: {}ms", objectId, (System.currentTimeMillis() - startTime));
                    }
                }
            }
        }
    }
}
