package com.jay.oss.storage.edit;

import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.edit.AbstractEditLogManager;
import com.jay.oss.common.edit.EditOperation;
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
 *  Storage端对象元数据editLog管理器
 * </p>
 *
 * @author Jay
 * @date 2022/03/01 14:30
 */
@Slf4j
public class StorageEditLogManager extends AbstractEditLogManager {
    private final MetaManager metaManager;

    public StorageEditLogManager(MetaManager metaManager){
        this.metaManager = metaManager;
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
                        case ADD: addObject(metaManager, content); count++;break;
                        case DELETE: deleteObject(metaManager, content);break;
                        default: break;
                    }
                }
            }
            log.info("load edit log finished, loaded: {} objects, time used: {}ms", count, (System.currentTimeMillis() - start));
            compress(metaManager);
            setLastFlushTime(System.currentTimeMillis());
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
        ByteBuf buffer = Unpooled.directBuffer((int)getChannel().size());
        for (FileMetaWithChunkInfo meta : snapshot) {
            buffer.writeByte(EditOperation.ADD.value());
            byte[] content = SerializeUtil.serialize(meta, FileMetaWithChunkInfo.class);
            buffer.writeInt(content.length);
            buffer.writeBytes(content);
        }
        buffer.readBytes(getChannel(), 0, buffer.readableBytes());
    }

    private void removeOldFile() throws IOException{
        FileChannel channel = getChannel();
        channel.close();
        File file = new File(OssConfigs.dataPath() + "/edit.log");
        if(file.delete() && file.createNewFile()){
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            setChannel(randomAccessFile.getChannel());
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
}
