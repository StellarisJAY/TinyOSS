package com.jay.oss.storage.persistence;

import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.entity.Bucket;
import com.jay.oss.common.entity.FileMeta;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.storage.meta.BucketManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

/**
 * <p>
 *  Bucket持久化工具
 *  快照持久化
 * </p>
 *
 * @author Jay
 * @date 2022/02/17 11:49
 */
@Slf4j
public class BucketPersistence {
    private final BucketManager bucketManager;

    public BucketPersistence(BucketManager bucketManager) {
        this.bucketManager = bucketManager;
    }

    /**
     * 持久化桶信息
     */
    public void saveBucket(){
        // 获取存储桶快照
        List<Bucket> buckets = bucketManager.snapshot();
        // 存储桶持久化文件目录
        String path = OssConfigs.dataPath() + "/meta/buckets.data";
        long start = System.currentTimeMillis();
        try(FileOutputStream outputStream = new FileOutputStream(path);
            FileChannel channel = outputStream.getChannel()){
            ByteBuf buffer = Unpooled.directBuffer();
            for (Bucket bucket : buckets) {
                // 序列化存储桶信息
                byte[] serialized = SerializeUtil.serialize(bucket, Bucket.class);
                // 写入序列化数组长度
                buffer.writeInt(serialized.length);
                // 写入序列化结果
                buffer.writeBytes(serialized);
                // listBucket
                List<FileMeta> metas = bucketManager.listBucket(bucket.getBucketName() + "-" + bucket.getAppId(), Integer.MAX_VALUE, 0);
                buffer.writeInt(metas.size());
                // 写入存储桶object记录
                for (FileMeta meta : metas) {
                    // 序列化
                    byte[] metaSerial = SerializeUtil.serialize(meta, FileMeta.class);
                    buffer.writeInt(metaSerial.length);
                    buffer.writeBytes(metaSerial);
                }
                buffer.readBytes(channel, buffer.readableBytes());
                buffer.clear();
            }
            buffer.release();
            channel.close();
            outputStream.close();
            log.info("buckets saved, count:{}, time used: {}ms", buckets.size(), (System.currentTimeMillis() - start));
        }catch (Exception e){
            log.error("bucket persistence error ", e);
        }
    }

    public void loadBucket(){
        String path = OssConfigs.dataPath() + "/meta/buckets.data";
        File file = new File(path);
        if(!file.exists()){
            log.info("no buckets persistence found, skipping load buckets");
            return;
        }
        long start = System.currentTimeMillis();
        int count = 0;
        try(FileInputStream inputStream = new FileInputStream(path);
            FileChannel channel = inputStream.getChannel()){
            ByteBuffer buffer = ByteBuffer.allocateDirect((int)channel.size());
            channel.read(buffer);
            buffer.rewind();
            while (buffer.hasRemaining()){
                int len = buffer.getInt();
                byte[] bytes = new byte[len];
                buffer.get(bytes);
                Bucket bucket = SerializeUtil.deserialize(bytes, Bucket.class);
                bucketManager.saveBucket(bucket);
                int metaCount = buffer.getInt();
                for(int i = 0; i < metaCount; i++){
                    int mLen = buffer.getInt();
                    byte[] serialized = new byte[mLen];
                    FileMeta meta = SerializeUtil.deserialize(serialized, FileMeta.class);
                    bucketManager.saveMeta(bucket.getBucketName() + "-" + bucket.getAppId(), meta);
                }
            }
            buffer.clear();
            channel.close();
            inputStream.close();
            log.info("load buckets finished, loaded: {}, time used: {} ms", count, (System.currentTimeMillis() - start));
        }catch (Exception e){
            log.error("load bucket persistence error ", e);
        }
    }
}
