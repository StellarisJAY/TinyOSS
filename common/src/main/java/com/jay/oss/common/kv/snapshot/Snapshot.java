package com.jay.oss.common.kv.snapshot;

import com.jay.oss.common.config.OssConfigs;
import com.jay.oss.common.util.StringUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 *  快照文件，内存中KV数据表的快照
 * </p>
 *
 * @author Jay
 * @date 2022/05/05 14:48
 */
public class Snapshot {
    private long timestamp;
    private final File file;
    private final FileChannel fileChannel;

    public Snapshot() throws Exception{
        timestamp = System.currentTimeMillis();
        String path = OssConfigs.dataPath() + File.separator + "snapshot-" + timestamp;
        this.file = new File(path);
        if(file.exists() || !file.createNewFile()){
            throw new RuntimeException("Can't create snapshot file: " + timestamp);
        }
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        this.fileChannel = randomAccessFile.getChannel();
    }

    public Snapshot(File file) throws Exception{
        this.file = file;
        if(!file.exists()){
            throw new RuntimeException("Snapshot file not exist ");
        }
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        this.fileChannel = randomAccessFile.getChannel();
    }

    public void write(ByteBuffer buffer) throws Exception {
        fileChannel.write(buffer);
    }

    public Map<String, byte[]> read() throws Exception{
        ByteBuffer buffer = ByteBuffer.allocateDirect((int)fileChannel.size());
        fileChannel.read(buffer);
        buffer.rewind();
        Map<String, byte[]> result = new HashMap<>(256);
        while(buffer.hasRemaining()){
            if(buffer.remaining() > 8){
                int keyLen = buffer.getInt();
                int valLen = buffer.getInt();
                if(buffer.remaining() >= keyLen + valLen){
                    byte[] keyBytes = new byte[keyLen];
                    byte[] value = new byte[valLen];
                    buffer.get(keyBytes);
                    buffer.get(value);
                    result.put(StringUtil.toString(keyBytes), value);
                }else{
                    break;
                }
            }else{
                break;
            }
        }
        return result;
    }

    public void delete() throws IOException {
        this.fileChannel.close();
        this.file.delete();
    }
}
