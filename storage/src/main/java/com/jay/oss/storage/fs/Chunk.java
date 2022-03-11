package com.jay.oss.storage.fs;

import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.edit.EditLog;
import com.jay.oss.common.edit.EditOperation;
import com.jay.oss.common.entity.FileMetaWithChunkInfo;
import com.jay.oss.common.entity.FilePart;
import com.jay.oss.common.util.SerializeUtil;
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
    public static final int COMPACT_THRESHOLD = 64 * 1024 * 1024;

    private int removedSize = 0;

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

    public void transferTo(Chunk target, int offset, int size) throws IOException {
        fileChannel.transferTo(offset, size, target.getFileChannel());
    }

    public void transferFrom(Chunk src, int offset, int size) throws IOException {
        fileChannel.transferFrom(src.getFileChannel(), offset, size);
    }

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
            if(removedSize >= COMPACT_THRESHOLD){
                // 压缩chunk文件
                return compact();
            }
            return null;
        }finally {
            readWriteLock.writeLock().unlock();
        }
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
            int removeStartOffset = -1;
            int removeEndOffset;
            // 已经删除的大小，用于改变后续文件的偏移量
            int removedSize = 0;
            int totalRemovedSize = 0;
            Iterator<FileMetaWithChunkInfo> iterator = objectList.iterator();
            // 遍历文件列表
            while(iterator.hasNext()){
                FileMetaWithChunkInfo meta = iterator.next();
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
                    ByteBuf buffer = Unpooled.directBuffer(size.get() - removeEndOffset);
                    // 将被删除部分之后的数据读入buffer
                    buffer.writeBytes(fileChannel, removeEndOffset, size.get() - removeEndOffset);
                    // 将buffer中数据写到被删除的起始位置
                    buffer.readBytes(fileChannel, removeStartOffset, size.get() - removeEndOffset);
                    // 重置删除起点
                    removeStartOffset = -1;
                    // 更新总删除数量
                    totalRemovedSize += removedSize;
                    // 更新chunk文件大小
                    this.size.set(size.get() - totalRemovedSize);
                }
            }
            this.removedSize = 0;
        }catch (IOException e){
            log.error("compression error: ", e);
        }
        return this.objectList;
    }

    public int getAndAddSize(int delta){
        return size.getAndAdd(delta);
    }
}
