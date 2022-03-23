package com.jay.oss.common.entity;

import com.jay.oss.common.acl.BucketAccessMode;
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
public class ListBucketRequest implements BucketAccessRequest {
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

    private BucketAccessMode accessMode;

    @Override
    public String bucket() {
        return bucket;
    }

    @Override
    public String token() {
        return token;
    }

    @Override
    public BucketAccessMode accessMode() {
        return accessMode;
    }
}
