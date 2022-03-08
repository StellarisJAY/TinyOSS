package com.jay.oss.common.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/03/07 15:37
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@ToString
public class LookupMultipartUploadRequest implements Serializable {
    private String uploadId;
    private String objectKey;
    private String bucket;
    private String token;
}
