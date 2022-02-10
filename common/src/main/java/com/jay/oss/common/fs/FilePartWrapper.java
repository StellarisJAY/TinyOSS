package com.jay.oss.common.fs;

import io.netty.buffer.ByteBuf;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/02/10 12:07
 */
@Builder
@Getter
@ToString
public class FilePartWrapper {
    private ByteBuf fullContent;
    @ToString.Exclude
    private byte[] key;
    private int index;
    private int partNum;
    private int length;
    private int keyLength;
}
