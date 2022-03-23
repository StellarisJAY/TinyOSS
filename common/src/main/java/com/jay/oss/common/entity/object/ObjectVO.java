package com.jay.oss.common.entity.object;

import lombok.*;

import java.io.Serializable;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/03/23 15:30
 */
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Getter
@Setter
@ToString
public class ObjectVO implements Serializable {
    private String objectKey;
    private String versionId;
    private Long size;
    private Long createTime;
    private String md5;
    private String fileName;
}
