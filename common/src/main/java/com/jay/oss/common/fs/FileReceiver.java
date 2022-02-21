package com.jay.oss.common.fs;

import com.jay.oss.common.entity.FileMeta;
import com.jay.oss.common.entity.FileMetaWithChunkInfo;
import com.jay.oss.common.entity.FilePart;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 *  文件接收器
 *  负责接收文件分片，并向chunk写入分片。
 * </p>
 *
 * @author Jay
 * @date 2022/01/21 14:52
 */
public class FileReceiver {
    /**
     * chunk 文件
     */
    private final Chunk chunk;
    /**
     * 剩余分片数量
     */
    private final AtomicInteger remainingParts;

    private final int offset;
    /**
     * chunk管理器。
     * 当最后一个分片写入成功后，需要将chunk归还给chunkManager
     */
    private final ChunkManager chunkManager;

    private FileReceiver(Chunk chunk, int parts, int offset, ChunkManager chunkManager) {
        this.chunk = chunk;
        this.remainingParts = new AtomicInteger(parts);
        this.chunkManager = chunkManager;
        this.offset = offset;
    }

    /**
     * 创建文件接收器
     * @param parts 分片个数
     * @param chunkManager {@link ChunkManager}
     * @return {@link FileReceiver}
     */
    public static FileReceiver createFileReceiver(Chunk chunk, int parts, int offset, ChunkManager chunkManager){
        return new FileReceiver(chunk, parts, offset, chunkManager);
    }

    /**
     * 接收文件分片
     * @param part {@link FilePart}
     * @return 是否已收到所有分片
     */
    public boolean receivePart(FilePart part){
        try{
            // 向chunk写入文件分片
            chunk.write(part, offset);
            // 检查是否是最后一个分片
            if(remainingParts.decrementAndGet() == 0){
                // 归还chunk
                chunkManager.offerChunk(chunk);
                return true;
            }
            return false;
        }catch (Exception e){
            // 写入失败，重试
            throw new RuntimeException("write part failed, part number: " + part.getPartNum(), e);
        }
    }
}
