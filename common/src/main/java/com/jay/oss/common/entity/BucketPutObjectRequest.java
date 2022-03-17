package com.jay.oss.common.entity;

import lombok.Builder;
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
 * @date 2022/02/17 14:28
 */
@Builder
@Getter
@Setter
@ToString
public class BucketPutObjectRequest implements Serializable {
    private String key;
    private String filename;
    private String bucket;
    private long size;
    private long createTime;
    private String md5;
    private String token;

}
