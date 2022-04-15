package com.jay.oss.storage.fs;

import com.jay.oss.common.config.OssConfigs;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
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
@Slf4j
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
    private AtomicInteger size;

    /**
     * chunk id
     */
    private final int id;
    /**
     * chunk size 128 MB = 4KB * 1024 * 32
     * 为了最大程度优化磁盘IO，Chunk文件的大小应该是磁盘块大小的整数倍。
     * Linux系统下，I/O Block大小是 4KB， 所以Chunk文件大小为4KB整数倍
     */
    public static final int MAX_BLOCK_SIZE = 128 * 1024 * 1024;

    private final ReentrantReadWriteLock readWriteLock;

    /**
     * Block头的长度，header主要用来记录block的大小等信息
     */
    private static final int BLOCK_HEADER_LENGTH = 8;

    private static final int BLOCK_HEADER_POSITION = MAX_BLOCK_SIZE - BLOCK_HEADER_LENGTH;

    public static final int INDEX_LENGTH = 12;

    private static final int BUFFER_SIZE = 256 * 1024;

    private static final int DELETE_MARK = -1;


    public Block(int blockId){
        this.readWriteLock = new ReentrantReadWriteLock();
        this.id =blockId;
        this.path = OssConfigs.dataPath() + File.separator + "block_" + blockId;
        this.size = new AtomicInteger(0);
        this.file = new File(path);
        try{
            RandomAccessFile rf = new RandomAccessFile(file, "rw");
            this.fileChannel = rf.getChannel();
            this.buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, MAX_BLOCK_SIZE);
        }catch (IOException e){
            throw new RuntimeException("Can't create block, id: " + id, e);
        }
    }

    public Block(File blockFile){
        String fileName = blockFile.getName();
        this.readWriteLock = new ReentrantReadWriteLock();
        this.id = Integer.parseInt(fileName.substring(fileName.indexOf("_") + 1));
        this.path = blockFile.getPath();
        this.file = blockFile;
        try{
            RandomAccessFile rf = new RandomAccessFile(file, "rw");
            this.fileChannel = rf.getChannel();
            this.buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, MAX_BLOCK_SIZE);
        }catch (IOException e){
            throw new RuntimeException("Can't create block, id: " + id, e);
        }
    }

    /**
     * 小文件适用的mmap写入方法
     * @param objectId objectId
     * @param src {@link ByteBuf}
     * @param length 写入内容长度
     * @return {@link ObjectIndex}
     */
    public ObjectIndex mmapWrite(long objectId, ByteBuf src, int length){
        try{
            readWriteLock.writeLock().lock();
            // 写入数据
            int offset = size.getAndAdd(length + INDEX_LENGTH);
            ByteBuffer slice = buffer.slice();
            slice.position(offset);
            slice.putLong(objectId);
            slice.putInt(length);
            slice.put(src.nioBuffer());
            return new ObjectIndex(this.id, offset, length);
        }finally {
            readWriteLock.writeLock().unlock();
        }
    }


    public ByteBuf mmapReadBytes(int offset, int start, int length){
        try{
            readWriteLock.readLock().lock();
            ByteBuf buffer = Unpooled.directBuffer(length);
            ByteBuffer slice = this.buffer.slice();
            int offset0 = offset + start + INDEX_LENGTH;
            slice.position(offset0);
            slice.limit(offset0 + length);
            buffer.writeBytes(slice);
            return buffer;
        }finally {
            readWriteLock.readLock().unlock();
        }
    }

    /**
     * 大文件写入方法，使用FileChannel和16KB缓冲区优化
     * @param objectId objectId
     * @param src {@link ByteBuf}
     * @param length 写入内容长度
     * @return {@link ObjectIndex}
     */
    public ObjectIndex fileChannelWriteBuffered(long objectId, ByteBuf src, int length){
        try{
            readWriteLock.writeLock().lock();
            int lengthWithIndex = length + INDEX_LENGTH;
            int offset0 = size.getAndAdd(lengthWithIndex);
            ByteBuf header = Unpooled.buffer(INDEX_LENGTH);
            header.writeLong(objectId);
            header.writeInt(length);
            ByteBuf fullBuffer = Unpooled.wrappedBuffer(header, src);
            int loop = (int)Math.ceil((double)lengthWithIndex / BUFFER_SIZE);
            for (int i = 0; i < loop; i++) {
                int offset = offset0 + i * BUFFER_SIZE;
                fullBuffer.readBytes(fileChannel, offset, i == loop - 1 ? fullBuffer.readableBytes() : BUFFER_SIZE);
            }
            return new ObjectIndex(id, offset0, length);
        } catch (IOException e) {
            log.warn("Write file channel failed ", e);
            return null;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }


    public ByteBuf fileChannelRead(int offset, int startPos, int length){
        try{
            readWriteLock.readLock().lock();
            ByteBuf result = Unpooled.directBuffer(length);
            int offset0 = offset + INDEX_LENGTH + startPos;
            result.writeBytes(fileChannel, offset0, length);
            return result;
        } catch (IOException e) {
           log.warn("Read file channel failed ", e);
           return null;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }


    public boolean delete(long objectId, int offset){
        try{
            readWriteLock.writeLock().lock();
            ByteBuffer slice = buffer.slice();
            slice.position(offset);
            if(slice.getLong() == objectId){
                slice.putInt(DELETE_MARK);
            }
            return true;
        }catch (Exception e){
            log.warn("Delete object in block failed, objectId: {}, offset: {}",objectId, offset, e);
            return false;
        } finally{
            readWriteLock.writeLock().unlock();
        }
    }

    /**
     * 启动时加载block文件的索引，获取到每个对象的位置
     * @return Map
     */
    public Map<Long, ObjectIndex> loadIndex(){
        Map<Long, ObjectIndex> indexes = new HashMap<>(16);
        ByteBuffer slice = buffer.slice();
        log.info("Slice limit: {}", slice.limit());
        int size = 0;
        while(slice.remaining() > INDEX_LENGTH){
            int offset = slice.position();
            long objectId = slice.getLong();
            int length = slice.getInt();
            if(objectId < 0 || length <= 0 || slice.remaining() < length){
                break;
            }
            // 跳过data部分
            slice.position(offset + length + INDEX_LENGTH);
            ObjectIndex index = new ObjectIndex(this.id, offset, length);
            indexes.put(objectId, index);
            size += length + INDEX_LENGTH;
        }
        this.size = new AtomicInteger(size);
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

    public int availableSpace(){
        return MAX_BLOCK_SIZE - this.size.get();
    }


}
