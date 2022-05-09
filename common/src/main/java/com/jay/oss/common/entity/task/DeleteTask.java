package com.jay.oss.common.entity.task;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/05/09 13:22
 */
@AllArgsConstructor
@Getter
public class DeleteTask implements Serializable {
    private long taskId;
    private long objectId;
}
