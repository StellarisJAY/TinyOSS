package com.jay.oss.common.entity.request;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/04/13 15:19
 */
@AllArgsConstructor
@Getter
@Setter
@ToString
public class GetObjectRequest implements Serializable {
    private long objectId;
    private int start;
    private int end;
}
