package com.jay.oss.common.entity.response;

import com.jay.oss.common.entity.task.DeleteTask;
import com.jay.oss.common.entity.task.ReplicaTask;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;
import java.util.List;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/05/09 14:32
 */
@AllArgsConstructor
@Getter
public class StorageHeartBeatResponse implements Serializable {
    private List<ReplicaTask> replicaTasks;
    private List<DeleteTask> deleteTasks;
}
