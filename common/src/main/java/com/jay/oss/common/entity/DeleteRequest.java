package com.jay.oss.common.entity;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

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
public class DeleteRequest {
    private String key;
}
