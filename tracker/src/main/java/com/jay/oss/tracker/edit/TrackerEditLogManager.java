package com.jay.oss.tracker.edit;

import com.jay.oss.common.bitcask.HintIndex;
import com.jay.oss.common.bitcask.Index;
import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.edit.AbstractEditLogManager;
import com.jay.oss.common.edit.EditOperation;
import com.jay.oss.common.prometheus.GaugeManager;
import com.jay.oss.common.util.KeyUtil;
import com.jay.oss.common.util.SerializeUtil;
import com.jay.oss.tracker.meta.BucketManager;
import com.jay.oss.tracker.track.MultipartUploadTracker;
import com.jay.oss.tracker.track.ObjectTracker;
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
            int bucketCount = 0;
            int objectCount = 0;
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
                        case ADD: saveBucket(content); bucketCount++;break;
                        case DELETE: deleteBucket(content);break;
                        case BUCKET_PUT_OBJECT: bucketPutObject(content); objectCount++; break;
                        case BUCKET_DELETE_OBJECT:bucketDeleteObject(content); objectCount--;break;
                        case MULTIPART_UPLOAD:saveMultipartUploadTask(content);break;
                        default: break;
                    }
                }
            }

            /*
                BitCask存储压缩
             */
            objectTracker.merge();
            bucketManager.merge();
            multipartUploadTracker.merge();
            // 重写压缩editLog
            compress();
            setLastSwapTime(System.currentTimeMillis());
            GaugeManager.getGauge("object_count").set(objectCount);
            log.info("edit log load and compressed, loaded bucket: {}, loaded object: {} time used: {}ms", bucketCount,objectCount, (System.currentTimeMillis() - start));
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

    /**
     * 打开重写channel
     * @throws IOException IOException
     */
    private void openRewriteChannel() throws IOException {
        String path = OssConfigs.dataPath() + File.separator + "rewrite" + System.currentTimeMillis() + ".log";
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
        File file = new File(OssConfigs.dataPath() + File.separator + "edit.log");
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
        List<String> buckets = bucketManager.listBuckets();
        for (String bucket : buckets) {
            Index index = bucketManager.getIndex(bucket);
            if(index != null && !index.isRemoved()){
                HintIndex hint = new HintIndex(bucket, index.getChunkId(), index.getOffset(), index.isRemoved());
                byte[] serialize = SerializeUtil.serialize(hint, HintIndex.class);
                buffer.writeByte(EditOperation.ADD.value());
                buffer.writeInt(serialize.length);
                buffer.writeBytes(serialize);
            }
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
        List<String> objects = objectTracker.listObject();
        for (String objectKey : objects) {
            Index index = objectTracker.getIndex(objectKey);
            if(index != null && !index.isRemoved()){
                HintIndex hint = new HintIndex(objectKey, index.getChunkId(), index.getOffset(), index.isRemoved());
                buffer.writeByte(EditOperation.BUCKET_PUT_OBJECT.value());
                byte[] content = SerializeUtil.serialize(hint, HintIndex.class);
                buffer.writeInt(content.length);
                buffer.writeBytes(content);
            }
        }
        buffer.readBytes(channel, buffer.readableBytes());
    }


    /**
     * 压缩MultipartUpload日志
     * 将未完成的MultipartUpload任务重写入日志
     * @throws IOException e
     */
    private void compressMultipartUploadLog() throws IOException {
        FileChannel channel = rewriteChannel;
        ByteBuf buffer = Unpooled.directBuffer();
        List<String> uploads = multipartUploadTracker.listUploads();
        for (String uploadId : uploads) {
            Index index = multipartUploadTracker.getIndex(uploadId);
            if(index != null && !index.isRemoved()){
                HintIndex hint = new HintIndex(uploadId, index.getChunkId(), index.getOffset(),index.isRemoved());
                byte[] serialized = SerializeUtil.serialize(hint, HintIndex.class);
                buffer.writeByte(EditOperation.MULTIPART_UPLOAD.value());
                buffer.writeInt(serialized.length);
                buffer.writeBytes(serialized);
            }
        }
        buffer.readBytes(channel, buffer.readableBytes());
    }

    /**
     * 加载添加存储同日志
     * @param content byte[]
     */
    private void saveBucket(byte[] content){
        HintIndex hint = SerializeUtil.deserialize(content, HintIndex.class);
        Index index = new Index(hint.getChunkId(), hint.getOffset(), hint.isRemoved());
        // 记录bitCask位置索引
        bucketManager.saveIndex(hint.getKey(), index);
    }


    /**
     * 加载删除存储桶日志
     * @param content byte[]
     */
    private void deleteBucket(byte[] content){
        log.info("deleted bucket: {}", content);
    }

    /**
     * 加载bucketPutObject日志
     * @param content byte[]
     */
    private void bucketPutObject(byte[] content){
        HintIndex hint = SerializeUtil.deserialize(content, HintIndex.class);
        String objectKey = hint.getKey();
        String bucket = KeyUtil.getBucket(objectKey);
        Index index = new Index(hint.getChunkId(), hint.getOffset(), hint.isRemoved());
        // 记录object位置
        objectTracker.saveObjectIndex(objectKey, index);
        // 存储桶记录object
        bucketManager.putObject(bucket, objectKey);
    }


    /**
     * 加载bucket删除object记录的日志
     * @param content byte[]
     */
    private void bucketDeleteObject(byte[] content){
        String objectKey = new String(content, OssConfigs.DEFAULT_CHARSET);
        String bucket = KeyUtil.getBucket(objectKey);
        // 删除object位置记录
        objectTracker.deleteObject(objectKey);
        // 存储桶删除object
        bucketManager.deleteObject(bucket, objectKey);
    }

    /**
     * 保存multipartUpload任务的index
     * @param content byte[]
     */
    private void saveMultipartUploadTask(byte[] content){
        HintIndex hint = SerializeUtil.deserialize(content, HintIndex.class);
        String uploadId = hint.getKey();
        Index index = new Index(hint.getChunkId(), hint.getOffset(), hint.isRemoved());
        multipartUploadTracker.saveIndex(uploadId, index);
    }

}
