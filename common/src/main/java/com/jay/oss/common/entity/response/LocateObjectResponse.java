package com.jay.oss.common.entity.response;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Set;

/**
 * <p>
 *  定位对象副本回复
 * </p>
 *
 * @author Jay
 * @date 2022/05/16 15:29
 */
@AllArgsConstructor
@Getter
@Setter
public class LocateObjectResponse implements Serializable {
    private long objectId;
    private Set<String> locations;
}
