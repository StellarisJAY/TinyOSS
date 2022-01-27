package com.jay.oss.common.fs;

import com.jay.oss.common.entity.FileMeta;
import com.jay.oss.common.entity.FileMetaWithChunkInfo;
import com.jay.oss.common.entity.FilePart;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
     * chunk file channel
     */
    private final FileChannel fileChannel;

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
    /**
     * 文件索引列表
     */
    private final HashMap<String, FileChunkIndex> files = new HashMap<>(128);

    public Chunk(int chunkId) {
        this.path = "D:/oss/chunk_"+chunkId;
        File file = new File(path);
        this.id = chunkId;
        try{
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            this.fileChannel = randomAccessFile.getChannel();
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
            FileChunkIndex chunkIndex = new FileChunkIndex();
            chunkIndex.setChunkId(this.id);
            chunkIndex.setOffset(this.size);
            chunkIndex.setSize(fileMeta.getSize());
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

    /**
     * 写入文件分片
     * @param part {@link FilePart}
     * @throws IOException IOException
     */
    public void write(FilePart part) throws IOException {
        FileLock lock = null;
        try{
            lock = fileChannel.lock();
            FileChunkIndex chunkIndex = files.get(part.getKey());
            // 计算该分片的位置
            int offset = chunkIndex.getOffset() + part.getPartNum() * FilePart.DEFAULT_PART_SIZE;
            ByteBuf data = part.getData();
            // 写入分片
            data.readBytes(fileChannel, offset);
        }finally {
            if(lock != null){
                lock.release();;
            }
        }
    }

    /**
     * 压缩chunk文件
     * @throws IOException IOException
     */
    public void compress() throws IOException {
        FileLock lock = null;
        try{
            // 加 文件排他锁
            lock = fileChannel.lock();
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
        }finally {
            // 释放排他锁
            if(lock != null){
                lock.release();
            }
        }

    }

    public void incrementSize(int delta){
        this.size += delta;
    }
}
