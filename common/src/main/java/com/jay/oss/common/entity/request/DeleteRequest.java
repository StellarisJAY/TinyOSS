package com.jay.oss.common.entity.request;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/02/12 12:52
 */
@Builder
@ToString
@Getter
public class DeleteRequest implements Serializable {
    private String key;
}
