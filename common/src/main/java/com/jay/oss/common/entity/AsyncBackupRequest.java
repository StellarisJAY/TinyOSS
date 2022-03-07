package com.jay.oss.common.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/03/07 11:21
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
public class AsyncBackupRequest {
    private String objectKey;
    private List<String> targets;
}
