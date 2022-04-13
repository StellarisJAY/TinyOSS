package com.jay.oss.common.entity.object;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

/**
 * <p>
 *  Object元数据
 * </p>
 *
 * @author Jay
 * @date 2022/03/17 12:10
 */
@Builder
@Getter
@Setter
@ToString
public class ObjectMeta implements Serializable {
    private String objectKey;
    private long objectId;
    private String versionId;
    private Long size;
    private Long createTime;
    private String md5;
    private String fileName;

    private String locations;
}
