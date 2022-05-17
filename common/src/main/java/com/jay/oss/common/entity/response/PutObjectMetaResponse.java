package com.jay.oss.common.entity.response;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/05/16 15:46
 */
@AllArgsConstructor
@Getter
@Setter
public class PutObjectMetaResponse implements Serializable {
    private long objectId;
    private List<String> locations;
    private String versionId;
}
