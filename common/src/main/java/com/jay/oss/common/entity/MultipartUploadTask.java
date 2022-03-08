package com.jay.oss.common.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/03/08 15:29
 */
@AllArgsConstructor
@Getter
@Setter
public class MultipartUploadTask implements Serializable {
    private String uploadId;
    private String objectKey;
    private String locations;
}
