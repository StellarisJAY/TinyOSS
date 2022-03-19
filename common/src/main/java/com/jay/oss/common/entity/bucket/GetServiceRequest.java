package com.jay.oss.common.entity.bucket;

import lombok.*;

import java.io.Serializable;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/03/19 14:15
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class GetServiceRequest implements Serializable {
    private int offset;
    private int count;
}
