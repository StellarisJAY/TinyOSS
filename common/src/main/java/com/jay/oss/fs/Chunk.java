package com.jay.oss.fs;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Iterator;
import java.util.LinkedList;

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
     * chunk size 128 MB = 4KB * 1024 * 32
     * 为了最大程度优化磁盘IO，Chunk文件的大小应该是磁盘块大小的整数倍。
     * Linux系统下，I/O Block大小是 4KB， 所以Chunk文件大小为4KB整数倍
     */
    public static final int MAX_CHUNK_SIZE = 4 * 1024 * 1024 * 32;
    /**
     * 文件索引列表
     */
    private final LinkedList<FileChunkMeta> files = new LinkedList<>();

    public Chunk(int chunkId) {
        this.path = "D:/oss/chunk_"+chunkId;
        File file = new File(path);
        try{
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            this.fileChannel = randomAccessFile.getChannel();
        }catch (IOException e){
            log.error("error creating chunk file: ", e);
            throw new RuntimeException("can't create a chunk file: " + e);
        }
    }

    /**
     * 向chunk文件写入数据
     * @param meta {@link FileChunkMeta} 写入数据信息
     * @param in {@link ByteBuf} 数据 buffer
     * @throws IOException IOException
     */
    public void write(FileChunkMeta meta, ByteBuf in) throws IOException {
        FileLock lock = null;
        try{
            // 加排他锁
            lock = fileChannel.lock();
            // 检查chunk剩余容量
            if(size + meta.getSize() <= MAX_CHUNK_SIZE){
                // 写入文件
                in.readBytes(fileChannel, size, meta.getSize());
                // 设置offset和size
                meta.setOffset(size);
                size += meta.getSize();
                files.add(meta);
            }else{
                throw new RuntimeException("no enough space in this chunk");
            }
        }finally {
            if(lock != null){
                lock.release();
            }
        }
    }

    /**
     * 从chunk文件读取数据
     * @param meta {@link FileChunkMeta} 待读取数据索引信息
     * @param out {@link ByteBuf} 目标buffer
     * @throws IOException IOException
     */
    public void read(FileChunkMeta meta, ByteBuf out) throws IOException {
        if(files.contains(meta) && !meta.isRemoved()){
            out.writeBytes(fileChannel, meta.getOffset(), meta.getSize());
        }
    }

    /**
     * 删除数据信息
     * @param meta {@link FileChunkMeta}
     */
    public void remove(FileChunkMeta meta){
        if(files.contains(meta)){
            meta.setRemoved(true);
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
            Iterator<FileChunkMeta> iterator = files.iterator();
            // 遍历文件列表
            while(iterator.hasNext()){
                FileChunkMeta meta = iterator.next();
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
}
