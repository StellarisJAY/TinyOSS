package com.jay.oss.common.entity;

import lombok.*;

import java.io.Serializable;

/**
 * <p>
 *  完成分片上传请求
 * </p>
 *
 * @author Jay
 * @date 2022/03/09 15:52
 */
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class CompleteMultipartUploadRequest implements Serializable {
    private String uploadId;
    private String objectKey;
    private String bucket;
    private String token;

    private String filename;
    private int parts;
    private String md5;
}
