package com.jay.oss.common.entity.task;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/05/09 13:19
 */
@AllArgsConstructor
@Getter
@Setter
public class ReplicaTask implements Serializable {
    private long taskId;
    private long objectId;
    private String storageUrl;
}
