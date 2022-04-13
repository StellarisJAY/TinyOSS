package com.jay.oss.common.entity.bucket;

import com.jay.oss.common.acl.Acl;
import com.jay.oss.common.acl.BucketAccessMode;
import com.jay.oss.common.entity.request.BucketAccessRequest;
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
 * @date 2022/04/01 16:39
 */
@AllArgsConstructor
@Getter
@Setter
@ToString
public class UpdateAclRequest implements BucketAccessRequest {
    private String bucket;
    private Acl acl;
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
