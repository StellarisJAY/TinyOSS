package com.jay.oss.common.kv.bitcask;

import lombok.*;

/**
 * <p>
 *  磁盘Hint文件的索引对象
 *  Index只在内存记录，使用Hash表形式存储，其中的Index对象为了减少空间开销并不具有Key属性。
 *  HintIndex的作用是写入磁盘的Hint持久化文件，所以它包括一个Key的所有信息
 * </p>
 *
 * @author Jay
 * @date 2022/03/18 10:09
 */
@Builder
@Getter
@Setter
@AllArgsConstructor
@ToString
public class HintIndex {
    private String key;
    private int chunkId;
    private int offset;

    private boolean removed;
}
