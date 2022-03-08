package com.jay.oss.common.entity;

import com.jay.oss.common.acl.Acl;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * <p>
 *  Bucket
 * </p>
 *
 * @author Jay
 * @date 2022/01/20 14:13
 */
@Getter
@Setter
@Builder
@ToString
public class Bucket implements Serializable {
    /**
     * 桶名称，不同用户可能创建相同名称的桶
     */
    private String bucketName;
    /**
     * appId，桶的唯一表示，为了避免相同名称的桶
     */
    private long appId;

    /**
     * ACL，private | public_read | public_write
     */
    private Acl acl;
    /**
     * 访问公钥
     */
    private String accessKey;
    /**
     * 访问密钥
     */
    private String secretKey;

    /**
     * 是否开启版本控制
     */
    private boolean versioning;
}
