package com.jay.oss.entity;

import com.jay.oss.fs.FileChunkIndex;
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
public class FileMeta {
    private String filename;
    private int size;
    private long createTime;
    private FileChunkIndex chunkIndex;
}
