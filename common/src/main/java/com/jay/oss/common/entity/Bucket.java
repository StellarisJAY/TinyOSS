package com.jay.oss.common.entity;

import lombok.Builder;
import lombok.Getter;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/01/20 14:13
 */
@Getter
@Builder
public class Bucket {
    private String bucketName;
    private String ownerName;
    private String ownerId;


}
