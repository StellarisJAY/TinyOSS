package com.jay.oss.common.bitcask;

import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.util.StringUtil;
import com.jay.oss.common.util.ThreadPoolUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutorService;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/04/07 11:51
 */
@Slf4j
public class HintLog {
    private final ByteBuf syncBuffer = Unpooled.directBuffer();
    private final ByteBuf flushBuffer = Unpooled.directBuffer();
    private int unFlushed = 0;
    private long lastFlushTime;
    private static final long FLUSH_INTERVAL = 10 * 1000;

    private final Object writeLock = new Object();
    private final Object flushLock = new Object();
    private static final int MAX_UN_FLUSHED = 64;

    /**
     * Hint日志文件channel
     */
    private FileChannel fileChannel;
    /**
     * 刷盘任务线程池
     */
    private final ExecutorService executor = ThreadPoolUtil.newSingleThreadPool("Async-Hint-Flush-");

    /**
     * 初始化Hint索引日志
     */
    public void init(){
        String path = OssConfigs.dataPath() + File.separator + "hint.log";
        try{
            // 创建Hint文件
            File file = new File(path);
            if(!file.exists() && !file.createNewFile()){
                throw new RuntimeException("Can't create Hint Index Log");
            }
            RandomAccessFile rf = new RandomAccessFile(file, "rw");
            this.fileChannel = rf.getChannel();
        }catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    /**
     * 追加hint索引
     * @param hintIndex {@link HintIndex}
     */
    public void append(HintIndex hintIndex){
        synchronized (writeLock){
            byte[] keyBytes = StringUtil.getBytes(hintIndex.getKey());
            syncBuffer.writeInt(keyBytes.length);
            syncBuffer.writeBytes(keyBytes);
            syncBuffer.writeInt(hintIndex.getChunkId());
            syncBuffer.writeInt(hintIndex.getOffset());
            swapBuffer(false);
        }
    }

    /**
     * 交换缓冲区并刷盘
     * @param force 是否强制刷盘
     */
    public void swapBuffer(boolean force){
        synchronized (writeLock){
            doSwapBuffer(force);
        }
    }

    /**
     * 交换缓冲区并flush到磁盘
     * @param force 强制刷盘
     */
    private void doSwapBuffer(boolean force){
        if(force || unFlushed >= MAX_UN_FLUSHED || System.currentTimeMillis() - lastFlushTime > FLUSH_INTERVAL){
            flushBuffer.ensureWritable(syncBuffer.readableBytes());
            syncBuffer.readBytes(flushBuffer, syncBuffer.readableBytes());
            syncBuffer.clear();
            executor.submit(this::flush);
            unFlushed = 0;
            lastFlushTime = System.currentTimeMillis();
        }
    }

    /**
     * 刷盘
     */
    private void flush(){
        synchronized (flushLock){
            try{
                long offset = fileChannel.size();
                flushBuffer.readBytes(fileChannel, offset, flushBuffer.readableBytes());
            }catch (IOException e){
                log.warn("Flush Hint log failed ", e);
            }
        }
    }

    /**
     * 压缩Hint索引日志
     */
    private void loadAndCompact(){

    }
}
