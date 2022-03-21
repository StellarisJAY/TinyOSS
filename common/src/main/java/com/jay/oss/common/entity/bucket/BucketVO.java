package com.jay.oss.common.entity.bucket;

import com.jay.oss.common.acl.Acl;
import lombok.*;

import java.io.Serializable;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/03/19 14:20
 */
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
@Getter
@Setter
public class BucketVO implements Serializable {
    private String bucketName;
    private Long appId;
    private Acl acl;
    private Boolean versioning;
}
