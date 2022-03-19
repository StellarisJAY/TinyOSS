package com.jay.oss.storage.fs;

import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.entity.FileMetaWithChunkInfo;
import com.jay.oss.common.entity.FilePart;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.DefaultFileRegion;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * <p>
 *  Chunk文件
 * </p>
 *
 * @author Jay
 * @date 2022/01/17 15:45
 */
@Slf4j
@Getter
@ToString
public class Chunk {
    /**
     * chunk file path
     */
    private final String path;

    /**
     * chunk file write channel
     */
    private final FileChannel fileChannel;

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
    public static final long MAX_CHUNK_SIZE = 4 * 1024 * 1024 * 32;

    private final ReentrantReadWriteLock readWriteLock;

    private final AtomicBoolean available = new AtomicBoolean(true);

    private final List<FileMetaWithChunkInfo> objectList = new ArrayList<>();

    /**
     * 执行Compact的阈值，当被删除对象的大小达到该阈值就会触发compact
     */
    public static final double COMPACT_THRESHOLD_PERCENT = 0.6;

    private int removedSize = 0;

    public static final long CHANNEL_CLOSE_THRESHOLD = 1024 * 1024;
    public static final long CHUNK_COMPRESSION_PERIOD = 30 * 1000;
    public Chunk(int chunkId) {
        this.path = OssConfigs.dataPath() + File.separator + "chunk_" +chunkId;
        this.file = new File(path);
        this.id = chunkId;
        this.size = new AtomicInteger(0);
        this.readWriteLock = new ReentrantReadWriteLock();
        try{
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            fileChannel = randomAccessFile.getChannel();
        }catch (IOException e){
            log.error("error creating chunk file: ", e);
            throw new RuntimeException("can't create a chunk file: " + e);
        }
    }

    public Chunk(String path, File file, int id) {
        this.path = path;
        this.file = file;
        this.size = new AtomicInteger((int)file.length());
        this.id = id;
        this.readWriteLock = new ReentrantReadWriteLock();
        try{
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            fileChannel = randomAccessFile.getChannel();
        }catch (IOException e){
            log.error("error creating chunk file: ", e);
            throw new RuntimeException("can't create a chunk file: " + e);
        }
    }

    public Chunk(String tempName){
        this.path = OssConfigs.dataPath() + File.separator + tempName;
        this.file = new File(path);
        this.size = new AtomicInteger(0);
        this.id = -1;
        this.readWriteLock = new ReentrantReadWriteLock();
        try{
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            fileChannel = randomAccessFile.getChannel();
        }catch (IOException e){
            log.error("error creating chunk file: ", e);
            throw new RuntimeException("can't create a chunk file: " + e);
        }
    }


    /**
     * 写入文件分片
     * @param part {@link FilePart}
     * @throws IOException IOException
     */
    public void write(FilePart part, int offset0) throws IOException {
        try{
            readWriteLock.writeLock().lock();
            checkAvailability();
            // 计算该分片的位置
            int offset = offset0 + part.getPartNum() * FilePart.DEFAULT_PART_SIZE;
            ByteBuf data = part.getData();
            // 写入分片
            data.readBytes(fileChannel, offset, data.readableBytes());
        }finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public void write(ByteBuf data) throws IOException {
        try{
            readWriteLock.writeLock().lock();
            checkAvailability();
            int length = data.readableBytes();
            data.readBytes(fileChannel, 0, length);
            size.set(length);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    /**
     * 读取文件，获取FileRegion
     * 用于下载时零拷贝传输
     * @param key object key
     * @param position 读取位置
     * @param length 读取长度
     * @return {@link DefaultFileRegion}
     */
    public DefaultFileRegion readFile(String key, int position, long length){
        RandomAccessFile rf;
        try{
            // 加 共享锁
            readWriteLock.readLock().lock();
            checkAvailability();
            // 创建RandomAccessFile，用Channel获取FileRegion
            rf = new RandomAccessFile(file, "r");
            // 返回FileRegion
            return new DefaultFileRegion(rf.getChannel(), position, length);
        } catch (IOException e) {
            log.error("read file {} error", key, e);
            return null;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public ByteBuf readFileBytes(String key, int position, long length){
        try{
            readWriteLock.readLock().lock();
            checkAvailability();
            ByteBuf buffer = Unpooled.directBuffer((int)length);
            buffer.writeBytes(fileChannel, position, (int) length);
            return buffer;
        } catch (IOException e) {
            log.error("read file {} error ", key, e);
            return null;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public int size(){
        return size.get();
    }

    @Deprecated
    public void transferTo(Chunk target, int offset, int size) throws IOException {
        fileChannel.transferTo(offset, size, target.getFileChannel());
    }

    /**
     * 从目标chunk文件传输数据到当前chunk文件
     * 零拷贝方法
     * @param src 源chunk
     * @param offset 转移起始偏移量
     * @param size 转移长度
     * @throws IOException IOException
     */
    public void transferFrom(Chunk src, int offset, int size) throws IOException {
        fileChannel.transferFrom(src.getFileChannel(), offset, size);
    }

    /**
     * 检查chunk文件是否可读可写
     * @throws IOException IOException
     */
    public void checkAvailability() throws IOException {
        if(!available.get()){
            throw new IOException("Chunk File Not Available");
        }
    }

    /**
     * 从chunk中删除一个对象
     * @param meta {@link FileMetaWithChunkInfo}
     */
    public List<FileMetaWithChunkInfo> deleteObject(FileMetaWithChunkInfo meta){
        try{
            readWriteLock.writeLock().lock();
            removedSize += meta.getSize();
            // 检查是否到达compact阈值
            if(removedSize >= compactThreshold()){
                removedSize = 0;
                // 压缩chunk文件
                return compact();
            }
            return null;
        }finally {
            readWriteLock.writeLock().unlock();
        }
    }

    private int compactThreshold(){
        return (int)(size.get() * COMPACT_THRESHOLD_PERCENT);
    }

    /**
     * 销毁一个chunk
     * 其中的数据会丢失
     */
    public void destroy(){
        try{
            readWriteLock.writeLock().lock();
            fileChannel.close();

            if(file.delete()){
                available.set(false);
            }else{
                log.error("Failed to Destroy chunk: {}", path);
            }
        } catch (IOException e) {
            log.error("Failed to Destroy Chunk: {}", path, e);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public void addObjectMeta(FileMetaWithChunkInfo meta){
        objectList.add(meta);
    }

    /**
     * 压缩chunk文件
     */
    public List<FileMetaWithChunkInfo> compact() {
        try{
            readWriteLock.writeLock().lock();
            int totalRemoved = 0;
            Iterator<FileMetaWithChunkInfo> iterator = objectList.iterator();
            while(iterator.hasNext()){
                FileMetaWithChunkInfo meta = iterator.next();
                // 修改offset，向前移
                meta.setOffset(meta.getOffset() - totalRemoved);
                if(meta.isRemoved()){
                    iterator.remove();
                    int startOffset = meta.getOffset();
                    int removeSize = (int)meta.getSize();
                    totalRemoved += removeSize;

                    ByteBuf buffer = Unpooled.directBuffer(size.get() - startOffset - removeSize);
                    // 将删除部分之后的数据读取到内存
                    buffer.writeBytes(fileChannel, startOffset + removeSize, size.get() - startOffset - removeSize);
                    // 将删除部分之后的数据写到被删除部分的位置
                    buffer.readBytes(fileChannel, startOffset, buffer.readableBytes());
                    size.set(size.get() - removeSize);
                }
            }
            if(totalRemoved > 0){
                // Truncate文件后面的被删除数据
                fileChannel.truncate(size.get());
            }
        }catch (IOException e){
            log.error("compression error: ", e);
        }finally {
            readWriteLock.writeLock().unlock();
        }
        return this.objectList;
    }

    public int getAndAddSize(int delta){
        return size.getAndAdd(delta);
    }
}
