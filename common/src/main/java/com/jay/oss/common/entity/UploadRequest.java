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
 * @date 2022/01/21 11:17
 */
@Getter
@Builder
@ToString
public class UploadRequest {
    private String key;
    private String filename;
    private String bucket;
    private long size;
    /**
     * 拥有者id
     */
    private String ownerId;
    /**
     * 文件的分片个数
     */
    private int parts;
}
