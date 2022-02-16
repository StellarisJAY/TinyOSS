package com.jay.oss.common.entity;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.Set;

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
public class Bucket {
    /**
     * 桶名称，不同用户可能创建相同名称的桶
     */
    private String bucketName;
    /**
     * appId，桶的唯一表示，为了避免相同名称的桶
     */
    private long appId;
    /**
     * 创建者ID
     */
    private String ownerId;

    /**
     * ACL，private | public_read | public_write
     */
    private String acl;
    /**
     * 访问公钥
     */
    private String accessKey;
    /**
     * 访问密钥
     */
    private String secretKey;
    /**
     * 读权限用户列表
     */
    private Set<String> readerList;
    /**
     * 写权限用户列表
     */
    private Set<String> writerList;
}
