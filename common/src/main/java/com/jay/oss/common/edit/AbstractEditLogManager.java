package com.jay.oss.common.edit;

import com.jay.oss.common.config.OssConfigs;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

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
     * 日志缓存
     * 编辑日志会先存储在缓存中，根据刷盘规则写入磁盘
     */
    private ByteBuf syncBuffer;
    /**
     * 日志文件channel
     */
    private FileChannel channel;
    private final Object writeLock = new Object();
    private volatile int unWrittenLogs = 0;
    private int maxUnWritten;
    private long lastFlushTime;

    @Override
    public void init() {
        this.maxUnWritten = 100;
        String path = OssConfigs.dataPath() + "\\edit.log";
        this.maxUnWritten = 100;
        try{
            File file = new File(path);
            if(!file.exists() && !file.createNewFile()){
                throw new RuntimeException("can't create edit log file");
            }
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            this.channel = randomAccessFile.getChannel();
            this.syncBuffer = Unpooled.buffer();
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
            flush(false);
        }
    }

    @Override
    public final void flush(boolean force) {
        // 判断是否到达刷盘阈值
        if(force || unWrittenLogs >= maxUnWritten || (System.currentTimeMillis() - lastFlushTime) >= OssConfigs.editLogFlushInterval()){
            try{
                int length = syncBuffer.readableBytes();
                // 写入channel
                int written = syncBuffer.readBytes(channel, length);
                // 判断是否完全写入
                if(written == length){
                    // 重置未写入数量 和 syncBuffer
                    unWrittenLogs = 0;
                    lastFlushTime = System.currentTimeMillis();
                    syncBuffer.clear();
                    log.info("edit log flushed, size: {} bytes", written);
                }
            }catch (Exception e){
                log.warn("flush edits log error ", e);
            }
        }
    }

    public FileChannel getChannel() {
        return channel;
    }

    public void setChannel(FileChannel channel) {
        this.channel = channel;
    }

    public void setLastFlushTime(long lastFlushTime) {
        this.lastFlushTime = lastFlushTime;
    }
}
