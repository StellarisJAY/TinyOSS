package com.jay.oss.common.entity.request;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/05/09 19:36
 */
@AllArgsConstructor
@Getter
public class StartCopyReplicaRequest implements Serializable {
    private long objectId;
    private String sourceUrl;
}
