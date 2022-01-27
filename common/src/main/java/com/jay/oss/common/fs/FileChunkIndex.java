package com.jay.oss.common.fs;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * <p>
 *  chunk文件索引信息
 * </p>
 *
 * @author Jay
 * @date 2022/01/18 10:20
 */
@Getter
@Setter
@ToString
public class FileChunkIndex {
    /**
     * chunk 文件路径
     */
    private int chunkId;
    /**
     * 该文件数据的offset
     */
    private int offset;
    /**
     * 文件大小
     */
    private long size;

    /**
     * 文件已被删除
     */
    private boolean removed;

}
