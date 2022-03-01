package com.jay.oss.storage.edit;

import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.entity.FileMetaWithChunkInfo;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.storage.meta.MetaManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.List;

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

    /**
     * 节点启动时加载日志和压缩日志
     * 加载元数据 同时根据日志删除元数据
     * @param metaManager {@link MetaManager}
     */
    public void loadAndCompress(MetaManager metaManager){
        try{
            if(channel.size() == 0){
                log.info("No Edit Log content found, skipping loading and compression");
                return;
            }
            ByteBuf buffer = Unpooled.directBuffer();
            buffer.writeBytes(channel, 0L, (int)channel.size());
            int count = 0;
            long start = System.currentTimeMillis();
            while(buffer.readableBytes() > 0){
                byte operation = buffer.readByte();
                int length = buffer.readInt();
                byte[] content = new byte[length];
                buffer.readBytes(content);
                EditOperation editOperation = EditOperation.get(operation);
                if(editOperation != null){
                    switch(editOperation){
                        case ADD: addObject(metaManager, content); count++;break;
                        case DELETE: deleteObject(metaManager, content);break;
                        default: break;
                    }
                }
            }
            log.info("load edit log finished, loaded: {} objects, time used: {}ms", count, (System.currentTimeMillis() - start));
            compress(metaManager);
        }catch (Exception e){
            log.warn("load and compress edit log error ", e);
        }
    }

    /**
     * 压缩日志
     * 将有效的日志数据重写入日志
     * @param metaManager {@link MetaManager}
     * @throws IOException IOException
     */
    private void compress(MetaManager metaManager) throws IOException {
        removeOldFile();
        List<FileMetaWithChunkInfo> snapshot = metaManager.snapshot();
        ByteBuf buffer = Unpooled.directBuffer((int)channel.size());
        for (FileMetaWithChunkInfo meta : snapshot) {
            buffer.writeByte(EditOperation.ADD.value());
            byte[] content = SerializeUtil.serialize(meta, FileMetaWithChunkInfo.class);
            buffer.writeInt(content.length);
            buffer.writeBytes(content);
        }
        buffer.readBytes(channel, 0, buffer.readableBytes());
    }

    private void removeOldFile() throws IOException{
        this.channel.close();
        File file = new File("D:/edit.log");
        if(file.delete() && file.createNewFile()){
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            this.channel = randomAccessFile.getChannel();
        }else{
            throw new RuntimeException("remove old log file failed");
        }
    }

    private void addObject(MetaManager metaManager, byte[] serialized){
        FileMetaWithChunkInfo meta = SerializeUtil.deserialize(serialized, FileMetaWithChunkInfo.class);
        metaManager.saveMeta(meta);
    }

    private void deleteObject(MetaManager metaManager, byte[] keyBytes){
        String key = new String(keyBytes, OssConfigs.DEFAULT_CHARSET);
        metaManager.delete(key);
    }

    public void append(EditLog editLog){
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
