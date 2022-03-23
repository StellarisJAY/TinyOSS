package com.jay.oss.common.entity;

import com.jay.oss.common.acl.BucketAccessMode;
import java.io.Serializable;

/**
 * <p>
 *  存储桶访问请求接口
 * </p>
 *
 * @author Jay
 * @date 2022/03/23 15:50
 */
public interface BucketAccessRequest extends Serializable {
    /**
     * 获取访问的存储桶
     * @return String
     */
    String bucket();

    /**
     * 获取访问Token
     * @return String
     */
    String token();

    /**
     * 获取访问模式
     * @return {@link BucketAccessMode}
     */
    BucketAccessMode accessMode();
}
