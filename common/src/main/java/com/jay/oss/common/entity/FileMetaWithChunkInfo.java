package com.jay.oss.common.entity;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/01/18 13:05
 */
@Getter
@Setter
@Builder
public class FileMetaWithChunkInfo {
    private String key;
    private long size;
    /**
     * chunk 文件路径
     */
    private int chunkId;
    /**
     * 该文件数据的offset
     */
    private int offset;

    /**
     * 文件被标记删除
     * 该标记表示文件已无法访问，但是数据还未被彻底删除
     */
    private boolean markRemoved;
    /**
     * 文件已被删除
     * 如果该标记为true，表示文件已经被彻底删除
     */
    private boolean removed;

    public void setRemoved(boolean removed){
        this.removed = removed;
    }

    public void setMarkRemoved(boolean removed){
        markRemoved = removed;
    }
}
