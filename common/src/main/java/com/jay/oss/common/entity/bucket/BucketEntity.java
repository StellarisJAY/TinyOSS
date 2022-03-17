package com.jay.oss.common.entity.bucket;

import lombok.*;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/03/17 10:33
 */
@Data
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
public class BucketEntity {
    /**
     * 桶名称，不同用户可能创建相同名称的桶
     */
    private String bucketName;
    /**
     * appId，桶的唯一表示，为了避免相同名称的桶
     */
    private Long appId;

    /**
     * ACL，private | public_read | public_write
     */
    private Byte acl;
    /**
     * 访问公钥
     */
    private String accessKey;
    /**
     * 访问密钥
     */
    private String secretKey;

    /**
     * 拥有者ID
     */
    private Long ownerId;

    /**
     * 创建时间戳
     */
    private Long createTime;

    /**
     * 是否开启版本控制
     */
    private Boolean versioning;
}
