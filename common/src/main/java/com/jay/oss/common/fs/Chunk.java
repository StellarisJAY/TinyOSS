package com.jay.oss.common.fs;

import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.entity.FilePart;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.DefaultFileRegion;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
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
    public static final long MAX_CHUNK_SIZE = 4 * 1024 * 1024 * 32;

    private final ReentrantReadWriteLock readWriteLock;

    /**
     * 未压缩标记
     */
    private final AtomicBoolean uncompressed = new AtomicBoolean(false);

    public static final long CHANNEL_CLOSE_THRESHOLD = 1024 * 1024;
    public static final long CHUNK_COMPRESSION_PERIOD = 30 * 1000;
    public Chunk(int chunkId) {
        this.path = OssConfigs.dataPath() + "/chunk_" +chunkId;
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
        this.path = OssConfigs.dataPath() + "/" + tempName;
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
            int length = data.readableBytes();
            data.readBytes(fileChannel, 0, length);
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
        try{
            // 加 共享锁
            readWriteLock.readLock().lock();
            // 创建RandomAccessFile，用Channel获取FileRegion
            RandomAccessFile rf = new RandomAccessFile(file, "r");
            // 返回FileRegion
            return new DefaultFileRegion(rf.getChannel(), position, length);
        } catch (FileNotFoundException e) {
            log.error("read file {} error", key, e);
            return null;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public ByteBuf readFileBytes(String key, int position, long length){
        try{
            readWriteLock.readLock().lock();
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

//    /**
//     * 压缩chunk文件
//     * @throws IOException IOException
//     */
//    public void compress()  {
//        try{
//            // 加 排他锁
//            readWriteLock.writeLock().lock();
//            int removeStartOffset = -1;
//            int removeEndOffset;
//            // 已经删除的大小，用于改变后续文件的偏移量
//            int removedSize = 0;
//            int totalRemovedSize = 0;
//            Iterator<Map.Entry<String, FileChunkIndex>> iterator = files.entrySet().iterator();
//            // 遍历文件列表
//            while(iterator.hasNext()){
//                FileChunkIndex meta = iterator.next().getValue();
//                // 更新文件偏移量
//                meta.setOffset(meta.getOffset() - totalRemovedSize);
//                // 判断是否被删除
//                if(meta.isRemoved()){
//                    iterator.remove();
//                    // 添加被删除大小
//                    removedSize += meta.getSize();
//                    if(removeStartOffset == -1){
//                        // 记录删除起点
//                        removeStartOffset = meta.getOffset();
//                    }
//                }else if(removeStartOffset != -1){
//                    removeEndOffset = meta.getOffset();
//                    ByteBuf buffer = Unpooled.directBuffer(size - removeEndOffset);
//                    // 将被删除部分之后的数据读入buffer
//                    buffer.writeBytes(fileChannel, removeEndOffset, size - removeEndOffset);
//                    // 将buffer中数据写到被删除的起始位置
//                    buffer.readBytes(fileChannel, removeStartOffset, size - removeEndOffset);
//                    // 重置删除起点
//                    removeStartOffset = -1;
//                    // 更新总删除数量
//                    totalRemovedSize += removedSize;
//                    // 更新chunk文件大小
//                    this.size -= removedSize;
//                }
//            }
//        }catch (IOException e){
//            log.error("compression error: ", e);
//        } finally {
//            // 重置待压缩标记
//            this.uncompressed.set(false);
//            // 释放排他锁
//            readWriteLock.writeLock().unlock();
//        }
//
//    }

    public int getAndAddSize(int delta){
        return size.addAndGet(delta);
    }

}
