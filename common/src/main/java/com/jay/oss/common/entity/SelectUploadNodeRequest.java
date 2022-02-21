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
 * @date 2022/02/21 11:22
 */
@Builder
@Getter
@ToString
public class SelectUploadNodeRequest {
    private String bucket;
    private String key;
    private String token;
    private long size;

}
