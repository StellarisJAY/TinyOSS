package com.jay.oss.common.fs;

import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.entity.FileMeta;
import com.jay.oss.common.entity.FileMetaWithChunkInfo;
import com.jay.oss.common.entity.FilePart;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.FileRegion;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private int size;

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
     * 文件索引列表
     */
    private final HashMap<String, FileChunkIndex> files = new HashMap<>(128);

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
        this.size = (int)file.length();
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

    /**
     * 向chunk添加文件
     * @param fileMeta {@link FileMeta} 文件元数据
     * @return {@link FileMetaWithChunkInfo}
     */
    public FileMetaWithChunkInfo addFile(FileMeta fileMeta){
        // 检查该文件大小是否超出chunk大小
        if(fileMeta.getSize() + this.size <= MAX_CHUNK_SIZE){
            // 创建文件的chunk索引
            FileChunkIndex chunkIndex = new FileChunkIndex(id, this.size, fileMeta.getSize(), false);
            // 记录文件索引
            this.files.put(fileMeta.getKey(), chunkIndex);
            // 更新chunk大小
            this.size += fileMeta.getSize();
            // 返回带有chunkInfo的文件元数据
            return FileMetaWithChunkInfo.builder().chunkIndex(chunkIndex)
                    .filename(fileMeta.getFilename())
                    .createTime(fileMeta.getCreateTime())
                    .key(fileMeta.getKey())
                    .size(fileMeta.getSize())
                    .build();
        }
        return null;
    }

    public void addFileChunkIndex(String key, FileChunkIndex chunkIndex){
        files.put(key, chunkIndex);
    }

    /**
     * 写入文件分片
     * @param part {@link FilePart}
     * @throws IOException IOException
     */
    public void write(FilePart part) throws IOException {
        try{
            readWriteLock.writeLock().lock();
            FileChunkIndex chunkIndex = files.get(part.getKey());
            // 计算该分片的位置
            int offset = chunkIndex.getOffset() + part.getPartNum() * FilePart.DEFAULT_PART_SIZE;
            ByteBuf data = part.getData();
            // 写入分片
            data.readBytes(fileChannel, offset, data.readableBytes());
        }finally {
            readWriteLock.writeLock().unlock();
        }
    }

    /**
     * 读取chunk中一个对象的指定范围内的数据
     * @param key object key
     * @param position 开始字节
     * @param length 读取长度
     * @param buffer output buffer
     * @throws IOException IOException
     */
    public void read(String key, int position, long length, ByteBuf buffer) throws IOException {
        // 获取object索引信息
        FileChunkIndex chunkIndex = files.get(key);
        // 获取object在chunk中的偏移
        int offset = chunkIndex.getOffset();
        try{
            // 加共享锁
            readWriteLock.readLock().lock();
            // 从fileChannel写入到buffer
            buffer.writeBytes(fileChannel, offset + position, (int)length);
        }finally {
            readWriteLock.readLock().unlock();
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
        // 获取object索引信息
        FileChunkIndex chunkIndex = files.get(key);
        int offset = chunkIndex.getOffset();
        try{
            // 加 共享锁
            readWriteLock.readLock().lock();
            // 创建RandomAccessFile，用Channel获取FileRegion
            RandomAccessFile rf = new RandomAccessFile(file, "r");
            // 返回FileRegion
            return new DefaultFileRegion(rf.getChannel(), offset + position, length);
        } catch (FileNotFoundException e) {
            log.error("read file {} error", key, e);
            return null;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    /**
     * 删除文件，仅设置删除标记
     * 磁盘的删除工作由compressor线程异步完成
     * @param key object key
     * @return 是否删除成功
     */
    public boolean delete(String key){
        try{
            // 加排他锁
            readWriteLock.writeLock().lock();
            FileChunkIndex remove = files.get(key);
            if(remove != null){
                // 设置该文件的删除标记
                remove.setRemoved(true);
                // 设置chunk文件待压缩标记
                this.uncompressed.set(true);
            }
            return remove != null;
        }
        finally {
            readWriteLock.writeLock().unlock();
        }
    }

    /**
     * 压缩chunk文件
     * @throws IOException IOException
     */
    public void compress()  {
        try{
            // 加 排他锁
            readWriteLock.writeLock().lock();
            int removeStartOffset = -1;
            int removeEndOffset;
            // 已经删除的大小，用于改变后续文件的偏移量
            int removedSize = 0;
            int totalRemovedSize = 0;
            Iterator<Map.Entry<String, FileChunkIndex>> iterator = files.entrySet().iterator();
            // 遍历文件列表
            while(iterator.hasNext()){
                FileChunkIndex meta = iterator.next().getValue();
                // 更新文件偏移量
                meta.setOffset(meta.getOffset() - totalRemovedSize);
                // 判断是否被删除
                if(meta.isRemoved()){
                    iterator.remove();
                    // 添加被删除大小
                    removedSize += meta.getSize();
                    if(removeStartOffset == -1){
                        // 记录删除起点
                        removeStartOffset = meta.getOffset();
                    }
                }else if(removeStartOffset != -1){
                    removeEndOffset = meta.getOffset();
                    ByteBuf buffer = Unpooled.directBuffer(size - removeEndOffset);
                    // 将被删除部分之后的数据读入buffer
                    buffer.writeBytes(fileChannel, removeEndOffset, size - removeEndOffset);
                    // 将buffer中数据写到被删除的起始位置
                    buffer.readBytes(fileChannel, removeStartOffset, size - removeEndOffset);
                    // 重置删除起点
                    removeStartOffset = -1;
                    // 更新总删除数量
                    totalRemovedSize += removedSize;
                    // 更新chunk文件大小
                    this.size -= removedSize;
                }
            }
        }catch (IOException e){
            log.error("compression error: ", e);
        } finally {
            // 重置待压缩标记
            this.uncompressed.set(false);
            // 释放排他锁
            readWriteLock.writeLock().unlock();
        }

    }

    public boolean isUncompressed(){
        return this.uncompressed.get();
    }

    public void incrementSize(long delta){
        this.size += delta;
    }
}
