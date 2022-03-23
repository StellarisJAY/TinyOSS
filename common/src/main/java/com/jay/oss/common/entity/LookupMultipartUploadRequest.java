package com.jay.oss.common.entity;

import com.jay.oss.common.acl.BucketAccessMode;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

/**
 * <p>
 *  查询分片上传任务请求
 * </p>
 *
 * @author Jay
 * @date 2022/03/07 15:37
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@ToString
public class LookupMultipartUploadRequest implements BucketAccessRequest {
    private String uploadId;
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
