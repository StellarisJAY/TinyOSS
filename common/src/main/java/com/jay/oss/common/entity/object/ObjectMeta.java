package com.jay.oss.common.entity.object;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

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
    private long objectId;
    private String versionId;
    private int size;
    private long createTime;
    private String md5;
    private String fileName;
}
