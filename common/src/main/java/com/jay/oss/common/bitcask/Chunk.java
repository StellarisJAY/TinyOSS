package com.jay.oss.common.bitcask;

import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.util.StringUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
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
    private int size;
    public static final int MAX_DATA_SIZE = 4 * 1024 * 1024;
    private final FileChannel activeChannel;
    private static final AtomicInteger ID_PROVIDER = new AtomicInteger(0);


    public Chunk(boolean merge, int chunkId) throws IOException {
        this.chunkId = chunkId;
        String path = OssConfigs.dataPath() + BitCaskStorage.CHUNK_DIRECTORY + File.separator + (merge ? "merged_chunks" : "chunk_" + chunkId);
        File file = new File(path);
        if(!file.getParentFile().exists() && !file.getParentFile().mkdirs()){
            throw new RuntimeException("can't make parent directory");
        }
        if(!file.exists() && !file.createNewFile()){
            throw new RuntimeException("can't create chunk file " + file);
        }
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        this.activeChannel = randomAccessFile.getChannel();
        this.size = (int)activeChannel.size();
    }

    private Chunk(int chunkId, int size, FileChannel channel){
        this.chunkId = chunkId;
        this.size = size;
        this.activeChannel = channel;
    }

    public static Chunk getChunkInstance(File file) throws Exception {
        String name = file.getName();
        int idx;
        if((idx = name.lastIndexOf("_")) != -1){
            int chunkId = Integer.parseInt(name.substring(idx + 1));
            int size = (int)file.length();
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            FileChannel channel = randomAccessFile.getChannel();
            return new Chunk(chunkId, size, channel);
        }
        return null;
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

    /**
     * 判断chunk是否处于不可写状态
     * @return boolean
     */
    public boolean isNotWritable(){
        return size >= MAX_DATA_SIZE;
    }

    /**
     * 扫描整个chunk文件，加载出索引信息
     * @return {@link Map} 索引信息
     */
    protected Map<String, Index> fullScanChunk() throws IOException {
        ByteBuf buffer = Unpooled.buffer();
        Map<String, Index> indexMap = new HashMap<>(256);
        buffer.writeBytes(activeChannel, 0, (int)activeChannel.size());
        while(buffer.isReadable()){
            int offset = buffer.readerIndex();
            int keyLen = buffer.readInt();
            int valLen = buffer.readInt();
            byte[] keyBytes = new byte[keyLen];
            buffer.readBytes(keyBytes);
            buffer.skipBytes(valLen);
            indexMap.put(StringUtil.toString(keyBytes), new Index(chunkId, offset, valLen == 1));
        }
        return indexMap;
    }

    /**
     * 关闭文件channel
     * @throws IOException e
     */
    protected void closeChannel() throws IOException {
        activeChannel.close();
    }

    @Override
    public String toString(){
        return "Chunk_" + chunkId;
    }
}
