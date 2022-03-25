package com.jay.oss.common.entity.request;

import com.jay.oss.common.acl.BucketAccessMode;
import lombok.*;

/**
 * <p>
 *  完成分片上传请求
 * </p>
 *
 * @author Jay
 * @date 2022/03/09 15:52
 */
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class CompleteMultipartUploadRequest implements BucketAccessRequest {
    private String uploadId;
    private String objectKey;
    private String bucket;
    private String token;

    private int size;
    private String filename;
    private int parts;
    private String md5;
    private String versionId;

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
