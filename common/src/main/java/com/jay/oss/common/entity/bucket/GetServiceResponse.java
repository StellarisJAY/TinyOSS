package com.jay.oss.common.entity.bucket;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/03/19 14:29
 */
@AllArgsConstructor
@Getter
@Setter
@NoArgsConstructor
public class GetServiceResponse implements Serializable {
    private List<BucketVO> buckets;
    private int total;
}
