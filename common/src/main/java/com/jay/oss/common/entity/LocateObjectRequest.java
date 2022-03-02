package com.jay.oss.common.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/03/02 12:02
 */
@AllArgsConstructor
@Getter
@Setter
@ToString
public class LocateObjectRequest {
    private String objectKey;
    private String bucket;
    private String token;
}
