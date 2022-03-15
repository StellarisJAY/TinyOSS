package com.jay.oss.storage.edit;

import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.edit.AbstractEditLogManager;
import com.jay.oss.common.edit.EditOperation;
import com.jay.oss.common.entity.FileMetaWithChunkInfo;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.storage.fs.Chunk;
import com.jay.oss.storage.fs.ChunkManager;
import com.jay.oss.storage.meta.MetaManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.List;

/**
 * <p>
 *  Storage端对象元数据editLog管理器
 * </p>
 *
 * @author Jay
 * @date 2022/03/01 14:30
 */
@Slf4j
public class StorageEditLogManager extends AbstractEditLogManager {
    private final MetaManager metaManager;
    private final ChunkManager chunkManager;

    /**
     * 临时channel，避免重写时覆盖原有EditLog
     */
    private FileChannel rewriteChannel;
    private File rewriteFile;
    public StorageEditLogManager(MetaManager metaManager, ChunkManager chunkManager){
        this.metaManager = metaManager;
        this.chunkManager = chunkManager;
    }
    @Override
    public void loadAndCompress() {
        try{
            FileChannel channel = getChannel();
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
                        case ADD: addObject(content); count++;break;
                        case DELETE: deleteObject(content);break;
                        default: break;
                    }
                }
            }
            log.info("load edit log finished, loaded: {} objects, time used: {}ms", count, (System.currentTimeMillis() - start));
            compress(metaManager);
            setLastSwapTime(System.currentTimeMillis());
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
        // 开启重写通道
        openRewriteChannel();
        List<FileMetaWithChunkInfo> snapshot = metaManager.snapshot();
        ByteBuf buffer = Unpooled.directBuffer();
        for (FileMetaWithChunkInfo meta : snapshot) {
            if(!meta.isRemoved()){
                buffer.writeByte(EditOperation.ADD.value());
                byte[] content = SerializeUtil.serialize(meta, FileMetaWithChunkInfo.class);
                buffer.writeInt(content.length);
                buffer.writeBytes(content);
            }
        }
        // 写入channel
        buffer.readBytes(rewriteChannel, buffer.readableBytes());
    }

    /**
     * 打开重写channel
     * @throws FileNotFoundException e
     */
    private void openRewriteChannel() throws FileNotFoundException {
        rewriteFile = new File(OssConfigs.dataPath() + "/rewrite" + System.currentTimeMillis() + ".log");
        RandomAccessFile rf = new RandomAccessFile(rewriteFile, "rw");
        rewriteChannel = rf.getChannel();
    }


    /**
     * 删除旧EditLog文件
     * @throws IOException IOException
     */
    private void removeOldFile() throws IOException{
        FileChannel channel = getChannel();
        channel.close();
        rewriteChannel.close();
        File file = new File(OssConfigs.dataPath() + "/edit.log");
        if(file.delete() && rewriteFile.renameTo(file)){
            RandomAccessFile rf = new RandomAccessFile(file, "rw");
            setChannel(rf.getChannel());
        }else{
            throw new RuntimeException("remove old log file failed");
        }
    }

    /**
     * 添加Object元数据到内存
     * @param serialized 序列化元数据
     */
    private void addObject(byte[] serialized){
        FileMetaWithChunkInfo meta = SerializeUtil.deserialize(serialized, FileMetaWithChunkInfo.class);
        metaManager.saveMeta(meta);
        // 找到object的chunk
        Chunk chunk = chunkManager.getChunkById(meta.getChunkId());
        if(chunk != null){
            // 将object对象放入chunk
            chunk.addObjectMeta(meta);
        }
    }

    /**
     * 删除object
     * 在内存元数据集合标记object已被删除
     * @param keyBytes key
     */
    private void deleteObject(byte[] keyBytes){
        String key = new String(keyBytes, OssConfigs.DEFAULT_CHARSET);
        FileMetaWithChunkInfo meta = metaManager.getMeta(key);
        if(meta != null){
            //设置object的删除标记
            meta.setRemoved(true);
        }
    }
}
