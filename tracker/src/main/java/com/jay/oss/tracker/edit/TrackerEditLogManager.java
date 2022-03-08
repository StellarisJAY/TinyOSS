package com.jay.oss.tracker.edit;

import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.edit.AbstractEditLogManager;
import com.jay.oss.common.edit.EditOperation;
import com.jay.oss.common.entity.Bucket;
import com.jay.oss.common.entity.MultipartUploadTask;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.tracker.meta.BucketManager;
import com.jay.oss.tracker.track.MultipartUploadTracker;
import com.jay.oss.tracker.track.ObjectTracker;
import com.jay.oss.common.bitcask.Index;
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
public class TrackerEditLogManager extends AbstractEditLogManager {


    private final ObjectTracker objectTracker;
    private final BucketManager bucketManager;
    private final MultipartUploadTracker multipartUploadTracker;

    public TrackerEditLogManager(ObjectTracker objectTracker, BucketManager bucketManager, MultipartUploadTracker multipartUploadTracker) {
        this.objectTracker = objectTracker;
        this.bucketManager = bucketManager;
        this.multipartUploadTracker = multipartUploadTracker;
    }

    @Override
    public void loadAndCompress() {
        try{
            FileChannel channel = getChannel();
            if(channel.size() == 0){
                log.info("No Bucket Edit log found, skipping loading Edit Log");
                return;
            }
                /*
                    加载过程，将所有的EditLog加载到内存中
                    并且删除DELETED的记录
                 */
            long start = System.currentTimeMillis();
            int count = 0;
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
                        case ADD: saveBucket(bucketManager, content); count++;break;
                        case DELETE: deleteBucket(content);break;
                        case BUCKET_PUT_OBJECT: bucketPutObject(content); break;
                        case BUCKET_DELETE_OBJECT:bucketDeleteObject(content);break;
                        case MULTIPART_UPLOAD:saveMultipartUploadTask(content);break;
                        default: break;
                    }
                }
            }
                /*
                    压缩EditLog
                    将内存中的EditLog重新写入硬盘，并将BitCask模型的chunk合并
                 */
            objectTracker.merge();
            // 压缩editLog
            compress(bucketManager);
            log.info("edit log load and compressed, loaded bucket: {}, time used: {}ms", count, (System.currentTimeMillis() - start));
        }catch (Exception e){
            log.error("load Bucket Edit Log Error ", e);
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
        }
        buffer.readBytes(channel, buffer.readableBytes());
        buffer.clear();
        // 获取所有的object index
        List<Index> indices = objectTracker.listIndexes();
        for (Index index : indices) {
            if(!index.isRemoved()){
                buffer.writeByte(EditOperation.BUCKET_PUT_OBJECT.value());
                byte[] content = SerializeUtil.serialize(index, Index.class);
                buffer.writeInt(content.length);
                buffer.writeBytes(content);
            }
        }

        List<MultipartUploadTask> tasks = multipartUploadTracker.listUploadTasks();
        for (MultipartUploadTask task : tasks) {
            buffer.writeByte(EditOperation.MULTIPART_UPLOAD.value());
            byte[] content = SerializeUtil.serialize(task, MultipartUploadTask.class);
            buffer.writeInt(content.length);
            buffer.writeBytes(content);
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


    private void deleteBucket(byte[] content){
        log.info("deleted bucket: {}", content);
    }

    private void bucketPutObject(byte[] content){
        Index index = SerializeUtil.deserialize(content, Index.class);
        String key = index.getKey();
        objectTracker.saveObjectIndex(key, index);
    }

    private void bucketDeleteObject(byte[] content){
        String key = new String(content, OssConfigs.DEFAULT_CHARSET);
        objectTracker.deleteObject(key);
    }

    private void saveMultipartUploadTask(byte[] content){
        MultipartUploadTask task = SerializeUtil.deserialize(content, MultipartUploadTask.class);
        multipartUploadTracker.saveUploadTask(task.getUploadId(), task);
    }

}
