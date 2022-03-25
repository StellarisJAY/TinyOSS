package com.jay.oss.common.entity.request;

import com.jay.oss.common.acl.BucketAccessMode;
import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;

/**
 * <p>
 *  检查访问权限请求
 * </p>
 *
 * @author Jay
 * @date 2022/02/17 10:57
 */
@Builder
@Getter
public class CheckBucketAclRequest implements Serializable {
    /**
     * 桶，bucketName-AppId
     */
    private String bucket;

    /**
     * 访问Token
     */
    private String token;

    /**
     * 访问模式，读、写、写权限
     */
    private BucketAccessMode accessMode;
}
