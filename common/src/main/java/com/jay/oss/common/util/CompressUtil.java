package com.jay.oss.common.util;

import com.jay.dove.compress.Compressor;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/03/17 14:14
 */
public class CompressUtil {

    private static final Compressor COMPRESSOR = new GzipCompressor();

    public static byte[] compress(byte[] content){
        return COMPRESSOR.compress(content);
    }

    public static byte[] decompress(byte[] content){
        return COMPRESSOR.decompress(content);
    }
}
