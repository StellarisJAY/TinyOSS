package com.jay.oss.common.entity;

import lombok.Builder;
import lombok.Getter;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/01/20 14:15
 */
@Builder
@Getter
public class FileMeta {
    /**
     * bucket-filename-version
     */
    private String key;
    private String filename;
    private long size;
    private long createTime;
}
