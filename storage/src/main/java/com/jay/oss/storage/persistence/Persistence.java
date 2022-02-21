package com.jay.oss.storage.persistence;

import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.entity.FileMetaWithChunkInfo;
import com.jay.oss.common.fs.Chunk;
import com.jay.oss.common.fs.ChunkManager;
import com.jay.oss.storage.meta.MetaManager;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * <p>
 *  持久化工具
 * </p>
 *
 * @author Jay
 * @date 2022/02/12 15:23
 */
@Slf4j
public class Persistence {

    private final MetaManager metaManager;
    private final ChunkManager chunkManager;

    public Persistence(MetaManager metaManager, ChunkManager chunkManager) {
        this.metaManager = metaManager;
        this.chunkManager = chunkManager;
    }

    /**
     * meta数据持久化
     * 持久化格式：
     * +----------+-------+--------+---------+--------+--------+-------+----------+-----------+
     * |  keyLen  |  key  |  fLen  |  fName  |  size  |  time  |  cId  |  offset  |  removed  |
     * +----------+-------+--------+---------+--------+--------+-------+----------+-----------+
     * keyLen：key长度，2bytes
     * key：keyLen bytes
     * fLen：filename长度，2bytes
     * fName：fLen bytes
     * size：文件大小 8 bytes
     * time：创建时间，8 bytes
     * cId：chunk id，4 bytes
     * offset：chunk偏移，4 bytes
     * removed：删除标记，1 byte
     * 总长度：keyLen + fLen + 29 bytes
     */
    public void persistenceMeta(){
        File file = new File(OssConfigs.dataPath() + "/meta/objects.data");
        if(!file.exists()){
            try{
                File parentFile = file.getParentFile();
                parentFile.mkdirs();
                file.createNewFile();
            }catch (Exception e){
                log.error("can't create persistence file for metas");
                return;
            }
        }
        try(FileOutputStream outputStream = new FileOutputStream(file);
            FileChannel fileChannel = outputStream.getChannel();){
            long start = System.nanoTime();
            List<FileMetaWithChunkInfo> snapshot = metaManager.snapshot();
            for (FileMetaWithChunkInfo meta : snapshot) {
                byte[] key = meta.getKey().getBytes(StandardCharsets.UTF_8);
                byte[] filename = meta.getFilename().getBytes(StandardCharsets.UTF_8);
                // 分配buffer，长度
                ByteBuffer buffer = ByteBuffer.allocate(key.length + filename.length + 29);
                // 文件key长度用short表示足够了，可以节省2个字节
                buffer.putShort((short)key.length);
                buffer.put(key);
                buffer.putShort((short)filename.length);
                buffer.put(filename);
                buffer.putLong(meta.getSize());
                buffer.putLong(meta.getCreateTime());
                buffer.putInt(meta.getChunkId());
                buffer.putInt(meta.getOffset());
                buffer.put(meta.isRemoved() ? (byte)1 : (byte)0);
                buffer.rewind();
                fileChannel.write(buffer);
            }
            log.info("object meta saved, time used: {} ms", (System.nanoTime() - start) / 1000000);
        }catch (IOException e){
            log.error("save meta persistence error ", e);
        }
    }

    /**
     * 加载meta persistence
     */
    public void loadMeta(){
        File file = new File(OssConfigs.dataPath() + "/meta/objects.data");
        try(FileInputStream inputStream = new FileInputStream(file);
            FileChannel fileChannel = inputStream.getChannel();){
            long start = System.nanoTime();
            int count = 0;
            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
            while(buffer.hasRemaining()){
                short keyLen = buffer.getShort();
                byte[] keyBytes = new byte[keyLen];
                buffer.get(keyBytes);
                short fLen = buffer.getShort();
                byte[] fileName = new byte[fLen];
                buffer.get(fileName);
                long size = buffer.getLong();
                long createTime = buffer.getLong();
                int chunkId = buffer.getInt();
                int offset = buffer.getInt();
                byte removed = buffer.get();
                String key = new String(keyBytes, StandardCharsets.UTF_8);
                // 创建文件元数据对象
                FileMetaWithChunkInfo meta = FileMetaWithChunkInfo.builder()
                        .key(key).filename(new String(fileName, StandardCharsets.UTF_8))
                        .createTime(createTime).size(size)
                        .chunkId(chunkId).offset(offset).removed(removed == 1).build();
                // 元数据管理器记录文件
                metaManager.saveMeta(meta);
                count++;
            }
            fileChannel.close();
            inputStream.close();
            log.info("load meta finished, loaded: {}, time used: {} ms", count, (System.nanoTime() - start) / (1000 * 1000));
        }catch (FileNotFoundException e){
            log.info("no meta persistence found, skipping loading meta");
        }catch (IOException e){
            log.error("loading meta persistence error ", e);
        }
    }

    public void loadChunk(){
        File file = new File(OssConfigs.dataPath());
        File[] chunkFiles = file.listFiles();
        if(chunkFiles == null){
            log.info("no chunk file found, skipping chunk loading");
            return;
        }
        long start = System.nanoTime();
        int count = 0;
        // 遍历chunk目录
        for(File chunkFile : chunkFiles){
            if(!chunkFile.isDirectory()){
                // 解析chunkID
                String name = chunkFile.getName();
                int chunkId = Integer.parseInt(name.substring(name.indexOf("_") + 1));
                // 创建chunk对象
                Chunk chunk = new Chunk(chunkFile.getPath(), chunkFile, chunkId);
                // 添加到chunkManager
                chunkManager.offerChunk(chunk);
                count ++;
            }
        }
        log.info("load chunk finished, loaded: {} chunks, time used: {} ms", count, (System.nanoTime() - start)/(1000000));
    }
}
