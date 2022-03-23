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
 * @date 2022/01/21 11:17
 */
@Getter
@Builder
@ToString
public class UploadRequest implements Serializable {
    private String key;
    private String filename;
    private long size;
    /**
     * 文件的分片个数
     */
    private int parts;
}
