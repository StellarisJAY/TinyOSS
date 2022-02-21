package com.jay.oss.common.entity;

import lombok.Builder;
import lombok.Getter;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/01/18 13:05
 */
@Getter
@Builder
public class FileMetaWithChunkInfo {
    private String key;
    private String filename;
    private long size;
    private long createTime;
    /**
     * chunk 文件路径
     */
    private int chunkId;
    /**
     * 该文件数据的offset
     */
    private int offset;
    /**
     * 文件已被删除
     */
    private boolean removed;

    public void setRemoved(boolean removed){
        this.removed = removed;
    }
}
