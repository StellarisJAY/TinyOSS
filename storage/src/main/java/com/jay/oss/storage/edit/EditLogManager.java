package com.jay.oss.storage.edit;

import com.jay.oss.storage.meta.BucketManager;
import com.jay.oss.storage.meta.MetaManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

/**
 * <p>
 *  编辑日志管理器
 * </p>
 *
 * @author Jay
 * @date 2022/02/20 11:39
 */
@Slf4j
public class EditLogManager {
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

    public void init(){
        String path = "D:/edit.log";
        this.maxUnWritten = 100;
        try{
            File file = new File(path);
            if(!file.exists()){
                file.createNewFile();
            }
            FileOutputStream outputStream = new FileOutputStream(file);
            this.channel = outputStream.getChannel();
            this.syncBuffer = Unpooled.buffer();
        }catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    public void loadAndCompress(MetaManager metaManager, BucketManager bucketManager){
        try{
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeBytes(channel, 0, (int)channel.size());
            while(buffer.readableBytes() > 0){
                long xid = buffer.readLong();
                byte type = buffer.readByte();
                byte operation = buffer.readByte();
                byte length = buffer.readByte();
                byte[] keyBytes = new byte[length];
                buffer.readBytes(keyBytes);
                String key = new String(keyBytes, StandardCharsets.UTF_8);
            }
        }catch (Exception e){
            log.warn("load edit log error ", e);
        }
    }

    public void append(EditLog log){
        /*
            因为是向同一个buffer追加数据，所以需要加锁避免多线程同时追加
         */
        synchronized (writeLock){
            // 写入syncBuffer
            syncBuffer.writeLong(log.getXid());
            syncBuffer.writeByte(log.getType());
            syncBuffer.writeByte(log.getOperation().value());
            byte[] key = log.getKey().getBytes(StandardCharsets.UTF_8);
            syncBuffer.writeByte(key.length);
            syncBuffer.writeBytes(key);
            unWrittenLogs ++;
            // 尝试刷盘
            flush();
        }
    }

    /**
     * 刷盘操作
     */
    private void flush(){
        // 判断是否到达刷盘阈值
        if(unWrittenLogs >= maxUnWritten){
            try{
                int length = syncBuffer.readableBytes();
                // 写入channel
                int written = syncBuffer.readBytes(channel, length);
                // 判断是否完全写入
                if(written == length){
                    // 重置未写入数量 和 syncBuffer
                    unWrittenLogs = 0;
                    syncBuffer.clear();
                }
            }catch (Exception e){
                log.warn("flush edits log error ", e);
            }
        }
    }
}
