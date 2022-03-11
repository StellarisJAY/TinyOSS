package com.jay.oss.common.entity;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * <p>
 *  删除存储桶object记录请求
 * </p>
 *
 * @author Jay
 * @date 2022/02/18 11:57
 */
@Builder
@Getter
@Setter
@ToString
public class DeleteObjectInBucketRequest implements Serializable {
    private String objectKey;
    private String bucket;
    private String token;
}
