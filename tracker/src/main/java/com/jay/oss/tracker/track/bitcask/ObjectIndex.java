package com.jay.oss.tracker.track.bitcask;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

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
@ToString
public class ObjectIndex {
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
