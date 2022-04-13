package com.jay.oss.storage.fs;

import com.jay.oss.common.config.OssConfigs;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.DefaultFileRegion;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * <p>
 *  Block文件是多个对象合并产生的
 *  block文件分为两部分，数据部分和头部分。
 *  数据部分是对象的实际数据
 *  头部分包括了block的大小和索引偏移量，头部大小为8字节
 *
 *  设对象数量为N，对象平均大小为Size，Block文件大小计算公式如下：
 *  BlockSize = (Size+16) * N + 8
 *  根据公式可以推出，当对象大小为1MB，128MB的block可以存127个对象
 * </p>
 *
 * @author Jay
 * @date 2022/04/12 15:37
 */
public class Block {
    /**
     * chunk file path
     */
    private final String path;

    /**
     * chunk file write channel
     */
    private final FileChannel fileChannel;

    private final MappedByteBuffer buffer;

    private final File file;

    /**
     * chunk current size
     */
    private final AtomicInteger size;

    /**
     * chunk id
     */
    private final int id;
    /**
     * chunk size 128 MB = 4KB * 1024 * 32
     * 为了最大程度优化磁盘IO，Chunk文件的大小应该是磁盘块大小的整数倍。
     * Linux系统下，I/O Block大小是 4KB， 所以Chunk文件大小为4KB整数倍
     */
    public static final int MAX_BLOCK_SIZE = 4 * 1024 * 1024 * 32;

    private final ReentrantReadWriteLock readWriteLock;

    private final AtomicBoolean available = new AtomicBoolean(true);

    /**
     * Block头的长度，header主要用来记录block的大小等信息
     */
    private static final int BLOCK_HEADER_LENGTH = 8;

    private static final int BLOCK_HEADER_POSITION = MAX_BLOCK_SIZE - BLOCK_HEADER_LENGTH;

    private static final int INDEX_LENGTH = 16;

    private final AtomicInteger indexOffset;


    public Block(int blockId){
        this.readWriteLock = new ReentrantReadWriteLock();
        this.id =blockId;
        this.path = OssConfigs.dataPath() + File.separator + "block_" + id;
        this.size = new AtomicInteger(0);
        this.file = new File(path);
        this.indexOffset = new AtomicInteger(MAX_BLOCK_SIZE - BLOCK_HEADER_LENGTH);
        try{
            RandomAccessFile rf = new RandomAccessFile(file, "rw");
            this.fileChannel = rf.getChannel();
            this.buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, MAX_BLOCK_SIZE);
        }catch (IOException e){
            throw new RuntimeException("Can't create block, id: " + id, e);
        }
    }

    public Block(String path, File blockFile, int blockId){
        this.readWriteLock = new ReentrantReadWriteLock();
        this.id =blockId;
        this.path = path;
        this.file = blockFile;
        try{
            RandomAccessFile rf = new RandomAccessFile(file, "rw");
            this.fileChannel = rf.getChannel();
            this.buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, MAX_BLOCK_SIZE);
            ByteBuffer slice = buffer.slice();
            slice.position(BLOCK_HEADER_POSITION);
            this.size = new AtomicInteger(slice.getInt());
            this.indexOffset = new AtomicInteger(slice.getInt());
        }catch (IOException e){
            throw new RuntimeException("Can't create block, id: " + id, e);
        }
    }

    public ObjectIndex write(long objectId, ByteBuf src, int length){
        try{
            readWriteLock.writeLock().lock();
            // 写入数据
            int offset = size.getAndAdd(length);
            ByteBuffer slice = buffer.slice();
            slice.position(offset);
            slice.put(src.nioBuffer());
            // 写入索引并更新索引偏移位置
            int indexPosition = indexOffset.addAndGet(-INDEX_LENGTH);
            slice.position(indexPosition);
            slice.putLong(objectId);
            slice.putInt(offset);
            slice.putInt(length);
            // 更新blockSize
            updateSizeAndIndexOffset();
            return new ObjectIndex(indexPosition, offset, length);
        }finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public DefaultFileRegion readFileRegion(int offset, int length){
        try{
            readWriteLock.readLock().lock();
            return new DefaultFileRegion(fileChannel, offset, length);
        }finally {
            readWriteLock.readLock().unlock();
        }
    }

    public ByteBuf readBytes(int offset, int length){
        try{
            readWriteLock.readLock().lock();
            ByteBuffer slice = buffer.slice();
            slice.position(offset);
            slice.limit(offset + length);
            return Unpooled.wrappedBuffer(slice);
        }finally {
            readWriteLock.readLock().unlock();
        }
    }

    /**
     * 判断剩余空间是否足够
     * @param length 需要的长度
     * @return boolean
     */
    public boolean isWritable(int length){
        return indexOffset.get() - size.get() - INDEX_LENGTH >= length;
    }

    /**
     * 启动时加载block文件的索引，获取到每个对象的位置
     * @return Map
     */
    public Map<Long, ObjectIndex> loadIndex(){
        ByteBuffer slice = buffer.slice();
        slice.position(indexOffset.get());
        slice.limit(BLOCK_HEADER_POSITION);
        Map<Long, ObjectIndex> indexes = new HashMap<>(16);
        while(slice.remaining() >= INDEX_LENGTH){
            int indexOffset = slice.position();
            long objectId = slice.getLong();
            int offset = slice.getInt();
            int size = slice.getInt();
            indexes.put(objectId, new ObjectIndex(indexOffset, offset, size));
        }
        return indexes;
    }

    public int getSize(){
        return size.get();
    }
    public int getId(){
        return id;
    }
    public int size(){
        return size.get();
    }
    public int getIndexOffset(){
        return indexOffset.get();
    }

    public int availableSpace(){
        return indexOffset.get() - size.get() - INDEX_LENGTH;
    }

    private void updateSizeAndIndexOffset(){
        ByteBuffer slice = buffer.slice();
        slice.position(BLOCK_HEADER_POSITION);
        slice.putInt(size.get());
        slice.putInt(indexOffset.get());
    }
}
