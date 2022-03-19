package com.jay.oss.common.edit;

import com.jay.oss.common.config.OssConfigs;
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
 *  EditLog管理器抽象
 * </p>
 *
 * @author Jay
 * @date 2022/03/01 14:26
 */
@Slf4j
public abstract class AbstractEditLogManager implements EditLogManager{
    /**
     * 同步缓存
     * 写日志时直接向该缓存写入
     * 当达到交换条件后，会将该缓存的内容复制到不可变缓存等待刷盘
     */
    private ByteBuf syncBuffer;
    /**
     * 不可变缓存
     * 该缓存用于向日志文件写入
     */
    private ByteBuf immutableBuffer;
    /**
     * 日志文件channel
     */
    private FileChannel channel;
    /**
     * 写入锁
     */
    private final Object writeLock = new Object();
    /**
     * 刷盘锁
     */
    private final Object flushLock = new Object();
    private volatile int unWrittenLogs = 0;
    private int maxUnWritten;
    private long lastSwapTime;

    /**
     * 异步flush线程池
     */
    private final ExecutorService asyncFlushExecutor = ThreadPoolUtil.newSingleThreadPool("Async-flush-");

    @Override
    public void init() {
        this.maxUnWritten = 100;
        String path = OssConfigs.dataPath() + File.separator + "edit.log";
        try{
            File file = new File(path);
            if(!file.exists() && !file.createNewFile()){
                throw new RuntimeException("can't create edit log file");
            }
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            this.channel = randomAccessFile.getChannel();
            channel.force(false);
            this.syncBuffer = Unpooled.buffer();
            this.immutableBuffer = Unpooled.buffer();
        }catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public final void append(EditLog editLog) {
        /*
            因为是向同一个buffer追加数据，所以需要加锁避免多线程同时追加
         */
        synchronized (writeLock){
            // 写入syncBuffer
            syncBuffer.writeByte(editLog.getOperation().value());
            syncBuffer.writeInt(editLog.getContent().length);
            syncBuffer.writeBytes(editLog.getContent());
            unWrittenLogs ++;
            // 尝试刷盘
            swapBuffer(false);
        }
    }

    @Override
    public final void append(EditLog[] editLogs){
        /*
            因为是向同一个buffer追加数据，所以需要加锁避免多线程同时追加
         */
        synchronized (writeLock){
            for (EditLog editLog : editLogs) {
                // 写入syncBuffer
                syncBuffer.writeByte(editLog.getOperation().value());
                syncBuffer.writeInt(editLog.getContent().length);
                syncBuffer.writeBytes(editLog.getContent());
                unWrittenLogs ++;
            }
            // 尝试交换缓冲区
            swapBuffer(false);
        }
    }

    @Override
    public final void flush() {
        synchronized (flushLock){
            if(immutableBuffer.readableBytes() > 0){
                try{
                    int length = immutableBuffer.readableBytes();
                    long offset = channel.size();
                    // 写入channel
                    int written = immutableBuffer.readBytes(channel, offset, length);
                    // 判断是否完全写入
                    if(written == length){
                        immutableBuffer.clear();
                        log.info("edit log flushed, size: {} bytes", written);
                    }
                }catch (Exception e){
                    log.warn("flush edits log error ", e);
                }
            }
        }
    }

    @Override
    public void swapBuffer(boolean force){
        synchronized(writeLock){
            doSwapBuffer(force);
        }
    }

    public void doSwapBuffer(boolean force){
        // 检查是否达到了交换缓存条件
        if(force || unWrittenLogs >= maxUnWritten || (System.currentTimeMillis() - lastSwapTime) >= OssConfigs.editLogFlushInterval()){
            // 转移到immutable buffer
            immutableBuffer.ensureWritable(syncBuffer.readableBytes());
            immutableBuffer.writeBytes(syncBuffer);
            syncBuffer.clear();
            unWrittenLogs = 0;
            lastSwapTime = System.currentTimeMillis();
            // 提交flush任务
            asyncFlushExecutor.submit(this::flush);
        }
    }

    public FileChannel getChannel() {
        return channel;
    }

    public void setChannel(FileChannel channel) {
        this.channel = channel;
    }

    public void setLastSwapTime(long lastSwapTime) {
        this.lastSwapTime = lastSwapTime;
    }

    @Override
    public final void close(){
        try{
            channel.close();
        }catch (IOException e){
            log.warn("failed to close channel");
        }
    }
}
