package com.jay.oss.common.entity;

import io.netty.buffer.ByteBuf;
import lombok.Builder;
import lombok.Getter;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/01/21 15:04
 */
@Builder
@Getter
public class FilePart {
    private String key;
    private int partNum;
    /**
     * 文件分片数据
     */
    private ByteBuf data;

    public static final int DEFAULT_PART_SIZE = 40 * 1024 * 1024;
}
