package com.jay.oss.common;

import com.jay.dove.compress.Compressor;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * <p>
 *  GZIP压缩工具
 * </p>
 *
 * @author Jay
 * @date 2022/03/17 14:13
 */
@Slf4j
public class GzipCompressor implements Compressor {

    public static final byte CODE = 1;

    @Override
    public byte[] compress(byte[] bytes) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try{
            GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream);
            gzipOutputStream.write(bytes);
            gzipOutputStream.close();
        }catch (IOException e){
            log.error("GZIP Compression Error ", e);
        }
        return outputStream.toByteArray();
    }

    @Override
    public byte[] decompress(byte[] bytes) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        try{
            GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream);
            byte[] buffer = new byte[1024];
            int length;
            while((length = gzipInputStream.read(buffer)) != -1){
                outputStream.write(buffer, 0, length);
            }
            gzipInputStream.close();
            inputStream.close();
        }catch (IOException e){
            log.error("GZIP Uncompress Error", e);
        }

        return outputStream.toByteArray();
    }

    @Override
    public byte getCode() {
        return CODE;
    }
}
