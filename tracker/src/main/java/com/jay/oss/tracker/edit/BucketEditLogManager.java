package com.jay.oss.tracker.edit;

import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.edit.AbstractEditLogManager;
import com.jay.oss.common.edit.EditOperation;
import com.jay.oss.common.entity.Bucket;
import com.jay.oss.common.entity.FileMeta;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.tracker.meta.BucketManager;
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
 *  Bucket 编辑日志管理器
 * </p>
 *
 * @author Jay
 * @date 2022/03/01 14:39
 */
@Slf4j
public class BucketEditLogManager extends AbstractEditLogManager {

    @Override
    public void loadAndCompress(Object manager) {
        if(manager instanceof BucketManager){
            BucketManager bucketManager = (BucketManager) manager;
            try{
                FileChannel channel = getChannel();
                if(channel.size() == 0){
                    log.info("No Bucket Edit log found, skipping loading Edit Log");
                    return;
                }
                long start = System.currentTimeMillis();
                ByteBuf buffer = Unpooled.directBuffer();
                buffer.writeBytes(channel, 0, (int)channel.size());
                // 读取editLog
                while(buffer.readableBytes() > 0){
                    byte operation = buffer.readByte();
                    int length = buffer.readInt();
                    byte[] content = new byte[length];
                    buffer.readBytes(content);
                    EditOperation editOperation = EditOperation.get(operation);
                    if(editOperation != null){
                        switch (editOperation){
                            case ADD: saveBucket(bucketManager, content);break;
                            case DELETE: deleteBucket(bucketManager, content);break;
                            case BUCKET_PUT_OBJECT: bucketPutObject(bucketManager, content); break;
                            case BUCKET_DELETE_OBJECT: bucketDeleteObject(bucketManager, content); break;
                            default: break;
                        }
                    }
                }
                // 压缩editLog
                compress(bucketManager);
                log.info("edit log load and compressed, time used: {}ms", (System.currentTimeMillis() - start));
            }catch (IOException e){
                log.error("load Bucket Edit Log Error ", e);
            }
        }
    }

    /**
     * 压缩editLog
     * @param bucketManager {@link BucketManager}
     * @throws IOException IOException
     */
    private void compress(BucketManager bucketManager) throws IOException {
        // 清空旧数据
        removeOldFile();
        FileChannel channel = getChannel();
        ByteBuf buffer = Unpooled.directBuffer();
        // 获取所有存储桶
        List<Bucket> buckets = bucketManager.snapshot();
        for (Bucket bucket : buckets) {
            // 写入日志
            buffer.writeByte(EditOperation.ADD.value());
            byte[] content = SerializeUtil.serialize(bucket, Bucket.class);
            buffer.writeInt(content.length);
            buffer.writeBytes(content);
            // 写入桶内所有元数据
            List<FileMeta> metas = bucketManager.listBucket(bucket.getBucketName(), 0, Integer.MAX_VALUE);
            for (FileMeta meta : metas) {
                buffer.writeByte(EditOperation.BUCKET_PUT_OBJECT.value());
                byte[] serialized = SerializeUtil.serialize(meta, FileMeta.class);
                buffer.writeInt(serialized.length);
                buffer.writeBytes(serialized);
            }
        }
        buffer.readBytes(channel, buffer.readableBytes());
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

    private void saveBucket(BucketManager bucketManager, byte[] content){
        Bucket bucket = SerializeUtil.deserialize(content, Bucket.class);
        bucketManager.saveBucket(bucket);
    }

    private void deleteBucket(BucketManager bucketManager, byte[] content){
        String bucketKey = new String(content, OssConfigs.DEFAULT_CHARSET);
    }

    private void bucketPutObject(BucketManager bucketManager, byte[] content){
        FileMeta meta = SerializeUtil.deserialize(content, FileMeta.class);
        String objectKey = meta.getKey();
        String bucketKey = objectKey.substring(0, objectKey.indexOf("/"));
        bucketManager.saveMeta(bucketKey, meta);
    }

    private void bucketDeleteObject(BucketManager bucketManager, byte[] content){
        String objectKey = new String(content, OssConfigs.DEFAULT_CHARSET);
        String bucketKey = objectKey.substring(0, objectKey.indexOf("/"));
        bucketManager.deleteMeta(bucketKey, objectKey);
    }
}
