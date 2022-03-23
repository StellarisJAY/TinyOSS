package com.jay.oss.common.entity.request;

import com.jay.oss.common.acl.BucketAccessMode;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/03/02 12:02
 */
@AllArgsConstructor
@Getter
@Setter
@ToString
public class LocateObjectRequest implements BucketAccessRequest {
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
