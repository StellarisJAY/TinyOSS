package com.jay.oss.common.entity.request;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

/**
 * <p>
 *  下载请求
 * </p>
 *
 * @author Jay
 * @date 2022/02/11 12:19
 */
@AllArgsConstructor
@Getter
@ToString
public class DownloadRequest implements Serializable {
    /**
     * object key
     */
    private String key;
    /**
     * 是否下载整个object
     */
    private boolean full;
    /**
     * Ranged下载参数，开始偏移
     */
    private int start;
    /**
     * Ranged下载参数，下载长度
     */
    private int length;
}
