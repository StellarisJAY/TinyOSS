package com.jay.oss.tracker.track.bitcask;

import lombok.*;

/**
 * <p>
 *  Object 位置信息index
 * </p>
 *
 * @author Jay
 * @date 2022/03/02 10:41
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class ObjectIndex {
    private String key;
    /**
     * chunk id
     */
    private int chunkId;
    /**
     * chunk内的偏移量
     */
    private int offset;
    /**
     * 删除标记
     */
    private boolean removed;
}
