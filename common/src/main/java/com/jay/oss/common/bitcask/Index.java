package com.jay.oss.common.bitcask;

import lombok.*;

/**
 * <p>
 *  BitCask存储模型索引
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
public class Index {
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
