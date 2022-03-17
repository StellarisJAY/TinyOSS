package com.jay.oss.tracker.edit;

import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.edit.AbstractEditLogManager;
import com.jay.oss.common.edit.EditOperation;
import com.jay.oss.common.entity.bucket.Bucket;
import com.jay.oss.common.entity.MultipartUploadTask;
import com.jay.oss.common.util.KeyUtil;
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
    /**
     * 日志重写Channel
     * 重写时创建新的文件重写，当重写完成后再将原有log删除
     */
    private FileChannel rewriteChannel;
    private File rewriteFile;

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
                // 读取操作类型
                byte operation = buffer.readByte();
                // 读取Log长度
                int length = buffer.readInt();
                // 读取日志content
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
            bucketManager.merge();
            // 压缩editLog
            compress();
            setLastSwapTime(System.currentTimeMillis());
            log.info("edit log load and compressed, loaded bucket: {}, time used: {}ms", count, (System.currentTimeMillis() - start));
        }catch (Exception e){
            log.error("load Bucket Edit Log Error ", e);
        }
    }

    /**
     * 压缩editLog
     * @throws IOException IOException
     */
    private void compress() throws IOException {
        // 创建重写日志文件
        openRewriteChannel();
        compressBucketLog();
        compressObjectLog();
        compressMultipartUploadLog();
        // 清空旧数据
        removeOldFile();
    }


    private void openRewriteChannel() throws IOException {
        String path = OssConfigs.dataPath() + "/rewrite" + System.currentTimeMillis() + ".log";
        rewriteFile = new File(path);
        if(rewriteFile.createNewFile()){
            RandomAccessFile rf = new RandomAccessFile(rewriteFile, "rw");
            this.rewriteChannel = rf.getChannel();
        }
    }


    /**
     * 删除原有的日志，替换日志channel为重写后的channel
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
     * 压缩存储桶日志
     * 将还未被删除的存储桶信息重写入日志
     * @throws IOException e
     */
    private void compressBucketLog() throws IOException {
        FileChannel channel = rewriteChannel;
        ByteBuf buffer = Unpooled.directBuffer();
        // 获取所有存储桶
        List<Index> indices = bucketManager.listIndexes();
        for (Index index : indices) {
            byte[] serialize = SerializeUtil.serialize(index, Index.class);
            buffer.writeByte(EditOperation.ADD.value());
            buffer.writeInt(serialize.length);
            buffer.writeBytes(serialize);
        }
        buffer.readBytes(channel, buffer.readableBytes());
        buffer.clear();
    }

    /**
     * 压缩object日志
     * 将没有被删除的object的信息重写入日志
     */
    private void compressObjectLog() throws IOException {
        FileChannel channel = rewriteChannel;
        ByteBuf buffer = Unpooled.directBuffer();
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
        buffer.readBytes(channel, buffer.readableBytes());
        buffer.clear();
    }

    /**
     * 压缩MultipartUpload日志
     * 将未完成的MultipartUpload任务重写入日志
     * @throws IOException e
     */
    private void compressMultipartUploadLog() throws IOException {
        FileChannel channel = rewriteChannel;
        ByteBuf buffer = Unpooled.directBuffer();
        List<MultipartUploadTask> tasks = multipartUploadTracker.listUploadTasks();
        for (MultipartUploadTask task : tasks) {
            buffer.writeByte(EditOperation.MULTIPART_UPLOAD.value());
            byte[] content = SerializeUtil.serialize(task, MultipartUploadTask.class);
            buffer.writeInt(content.length);
            buffer.writeBytes(content);
        }
        buffer.readBytes(channel, buffer.readableBytes());
    }


    private void saveBucket(BucketManager bucketManager, byte[] content){
        Index index = SerializeUtil.deserialize(content, Index.class);
        // 记录bitCask位置索引
        bucketManager.saveIndex(index.getKey(), index);
    }


    private void deleteBucket(byte[] content){
        log.info("deleted bucket: {}", content);
    }

    private void bucketPutObject(byte[] content){
        Index index = SerializeUtil.deserialize(content, Index.class);
        String objectKey = index.getKey();
        String bucket = KeyUtil.getBucket(objectKey);
        // 记录object位置
        objectTracker.saveObjectIndex(objectKey, index);
        // 存储桶记录object
        bucketManager.putObject(bucket, objectKey);
    }

    private void bucketDeleteObject(byte[] content){
        String objectKey = new String(content, OssConfigs.DEFAULT_CHARSET);
        String bucket = KeyUtil.getBucket(objectKey);
        // 删除object位置记录
        objectTracker.deleteObject(objectKey);
        // 存储桶删除object
        bucketManager.deleteObject(bucket, objectKey);
    }

    private void saveMultipartUploadTask(byte[] content){
        MultipartUploadTask task = SerializeUtil.deserialize(content, MultipartUploadTask.class);
        multipartUploadTracker.saveUploadTask(task.getUploadId(), task);
    }

}
