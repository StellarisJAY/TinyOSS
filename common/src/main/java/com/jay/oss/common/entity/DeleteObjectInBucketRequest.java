package com.jay.oss.common.entity;

import com.jay.oss.common.acl.BucketAccessMode;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

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
public class DeleteObjectInBucketRequest implements BucketAccessRequest {
    private String objectKey;
    private String bucket;
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
