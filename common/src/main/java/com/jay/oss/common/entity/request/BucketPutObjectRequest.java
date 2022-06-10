package com.jay.oss.common.entity.request;

import com.jay.oss.common.acl.BucketAccessMode;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

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
public class BucketPutObjectRequest implements BucketAccessRequest {
    private String key;
    private String filename;
    private String bucket;
    private int size;
    private long createTime;
    private String md5;
    private String token;

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
