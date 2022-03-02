package com.jay.oss.tracker.track.bitcask;

import com.jay.oss.common.config.OssConfigs;
import lombok.Getter;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 *  BitCask chunk
 * </p>
 *
 * @author Jay
 * @date 2022/03/02 10:43
 */
@Getter
public class Chunk {
    private final int chunkId;
    private int count;
    private int size;
    public static final int MAX_DATA_COUNT = 1024;
    private final FileChannel activeChannel;
    private static final AtomicInteger ID_PROVIDER = new AtomicInteger(0);

    public Chunk() throws IOException {
        this.count = 0;
        this.chunkId = ID_PROVIDER.getAndIncrement();
        this.size = 0;
        String path = OssConfigs.dataPath() + "/chunks/chunk_" + chunkId;
        File file = new File(path);
        if(!file.getParentFile().exists() && !file.getParentFile().mkdirs()){
            throw new RuntimeException("can't make parent directory");
        }
        if(!file.createNewFile()){
            throw new RuntimeException("can't create chunk file");
        }
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        this.activeChannel = randomAccessFile.getChannel();
    }

    /**
     * 写入chunk
     * 按照BitCask方式追加在文件末尾
     * 数据格式如下：
     * +----------+----------+-------+---------+
     * |  keyLen  |  valLen  |  key  |  value  |
     * +----------+----------+-------+---------+
     * @param key key
     * @param value value
     * @return offset
     * @throws IOException e
     */
    public int write(byte[] key, byte[] value) throws IOException {
        int offset = size;
        ByteBuffer buffer = ByteBuffer.allocate(8 + key.length + value.length);
        buffer.putInt(key.length);
        buffer.putInt(value.length);
        buffer.put(key);
        buffer.put(value);
        buffer.rewind();
        int written = activeChannel.write(buffer);
        count++;
        size += written;
        return offset;
    }

    /**
     * 读取一条数据
     * @param offset 数据在chunk中的offset
     * @return byte[]
     * @throws IOException e
     */
    public byte[] read(int offset) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        activeChannel.read(buffer, offset);
        buffer.rewind();
        int keyLen = buffer.getInt();
        int valueLen = buffer.getInt();
        ByteBuffer valueBuffer = ByteBuffer.allocate(valueLen);
        activeChannel.read(valueBuffer, offset + 8 + keyLen);
        valueBuffer.rewind();
        byte[] value = new byte[valueLen];
        valueBuffer.get(value);
        return value;
    }

    public boolean isWritable(){
        return count < MAX_DATA_COUNT;
    }
}
