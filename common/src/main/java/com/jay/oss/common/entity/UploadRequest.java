package com.jay.oss.common.entity;

import lombok.Builder;
import lombok.Getter;

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
public class UploadRequest {
    private String key;
    private String filename;
    private int size;
    /**
     * 拥有者id
     */
    private String ownerId;
    /**
     * 文件的分片个数
     */
    private int parts;
}
