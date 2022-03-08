package com.jay.oss.common.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * <p>
 *  List Bucket请求
 * </p>
 *
 * @author Jay
 * @date 2022/02/16 14:21
 */
@AllArgsConstructor
@Getter
@Setter
public class ListBucketRequest implements Serializable {
    /**
     * 桶名称
     */
    private String bucket;

    /**
     * 访问token
     */
    private String token;

    /**
     * list数量
     */
    private int count;

    /**
     * 偏移量
     */
    private int offset;
}
