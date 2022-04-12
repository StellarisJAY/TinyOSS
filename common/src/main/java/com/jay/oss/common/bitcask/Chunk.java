package com.jay.oss.common.bitcask;

import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.util.StringUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
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
@Slf4j
public class Chunk {
    private final int chunkId;
    private int size;
    public static final int MAX_DATA_SIZE = 1024 * 1024 * 64;
    private final FileChannel activeChannel;
    private final MappedByteBuffer mappedByteBuffer;
    private static final AtomicInteger ID_PROVIDER = new AtomicInteger(0);

    /**
     * 创建新的chunk
     * @param chunkId chunkID
     * @return {@link Chunk}
     * @throws IOException IOException
     */
    public static Chunk getNewChunk(int chunkId) throws IOException {
        return new Chunk(false, chunkId);
    }

    /**
     * 创建合并chunk
     * @return {@link Chunk}
     * @throws IOException IOException
     */
    public static Chunk getMergeChunkInstance() throws IOException {
        return new Chunk(true, 0);
    }

    /**
     * 通过文件获取Chunk对象
     * @param file {@link File} chunk文件
     * @return {@link Chunk}
     * @throws Exception Exception
     */
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

    private Chunk(boolean merge, int chunkId) throws IOException {
        this.chunkId = chunkId;
        String path = OssConfigs.dataPath() + BitCaskStorage.CHUNK_DIRECTORY + File.separator + (merge ? "merged_chunks" : "chunk_" + chunkId);
        File file = new File(path);
        ensureChunkFilePresent(file);
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        this.activeChannel = randomAccessFile.getChannel();
        this.mappedByteBuffer = merge ? null : activeChannel.map(FileChannel.MapMode.READ_WRITE, 0, MAX_DATA_SIZE);
        this.size = 0;
    }

    private Chunk(int chunkId, int size, FileChannel channel) throws IOException {
        this.chunkId = chunkId;
        this.size = size;
        this.activeChannel = channel;
        this.mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, size);
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
    public boolean isNotWritable(int length){
        return size + length >= MAX_DATA_SIZE;
    }

    /**
     * 扫描整个chunk文件，加载出索引信息
     * @return {@link Map} 索引信息
     */
    protected Map<String, Index> fullScanChunk() {
        ByteBuffer buffer = this.mappedByteBuffer.slice();
        Map<String, Index> indexMap = new HashMap<>(256);
        while(buffer.hasRemaining()){
            if(buffer.remaining() > 8){
                int offset = buffer.position();
                int keyLen = buffer.getInt();
                int valLen = buffer.getInt();
                if(keyLen > 0 && valLen > 0 && buffer.remaining() >= keyLen + valLen){
                    byte[] keyBytes = new byte[keyLen];
                    buffer.get(keyBytes, 0, keyLen);
                    buffer.position(buffer.position() + valLen);
                    indexMap.put(StringUtil.toString(keyBytes), new Index(chunkId, offset, valLen == 1));
                }else{
                    break;
                }
            }else{
                break;
            }
        }
        return indexMap;
    }

    /**
     * 关闭文件channel
     * @throws IOException e
     */
    protected void closeChannel() throws IOException {
        if(mappedByteBuffer != null){
            ((DirectBuffer)mappedByteBuffer).cleaner().clean();
        }
        activeChannel.close();
    }

    @Override
    public String toString(){
        return "Chunk_" + chunkId;
    }

    public int writeMmap(byte[] key, byte[] value){
        int keyLen = key.length;
        int valLen = value.length;
        mappedByteBuffer.position(size);
        mappedByteBuffer.putInt(keyLen);
        mappedByteBuffer.putInt(valLen);
        mappedByteBuffer.put(key);
        mappedByteBuffer.put(value);
        int offset = size;
        size += (8 + keyLen + valLen);
        mappedByteBuffer.rewind();
        return offset;
    }

    public byte[] readMmap(int offset){
        ByteBuffer slice = mappedByteBuffer.slice();
        slice.position(offset);
        if(slice.remaining() > 8){
            int keyLen = slice.getInt();
            int valLen = slice.getInt();
            if(keyLen > 0 && valLen > 0 && slice.remaining() >= keyLen + valLen){
                byte[] key = new byte[keyLen];
                byte[] value = new byte[valLen];
                slice.get(key, 0, keyLen);
                slice.get(value, 0, valLen);
                return value;
            }
        }
        return null;
    }

    /**
     * 保证chunk文件存在
     * @param file {@link File}
     * @throws IOException IOException
     */
    private void ensureChunkFilePresent(File file) throws IOException {
        if(!file.getParentFile().exists() && !file.getParentFile().mkdirs()){
            throw new RuntimeException("can't make parent directory");
        }
        if(!file.exists() && !file.createNewFile()){
            throw new RuntimeException("can't create chunk file " + file);
        }
    }

}
