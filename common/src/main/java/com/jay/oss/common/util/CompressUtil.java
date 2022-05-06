package com.jay.oss.common.util;

import com.jay.dove.compress.Compressor;

/**
 * <p>
 *  压缩工具
 * </p>
 *
 * @author Jay
 * @date 2022/03/17 14:14
 */
public class CompressUtil {

    private static final Compressor COMPRESSOR = new GzipCompressor();

    /**
     * 压缩数据
     * @param content 源数据
     * @return byte[]
     */
    public static byte[] compress(byte[] content){
        return COMPRESSOR.compress(content);
    }

    /**
     * 解压数据
     * @param content 压缩数据
     * @return byte[]
     */
    public static byte[] decompress(byte[] content){
        return COMPRESSOR.decompress(content);
    }
}
