package com.jay.oss.common.kv;

import com.jay.oss.common.util.StringUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>
 *  EditLog文件
 * </p>
 *
 * @author Jay
 * @date 2022/05/06 14:30
 */
public class EditLogFile {
    /**
     * 文件起始事务ID
     */
    private long minTxId;
    /**
     * 文件最大事务ID
     */
    private long maxTxId;
    /**
     * 刷盘缓冲区
     */
    private ByteBuf flushBuffer;
    private final AtomicBoolean inMemory = new AtomicBoolean(true);
    private RandomAccessFile rf;
    private FileChannel fileChannel;

    public EditLogFile(List<EditLog> inMemoryEditLogs) {
        this.minTxId = inMemoryEditLogs.get(0).getTxId();
        this.maxTxId = inMemoryEditLogs.get(inMemoryEditLogs.size()-1).getTxId();
        this.flushBuffer = Unpooled.buffer();
        for (EditLog editLog : inMemoryEditLogs) {
            flushBuffer.writeLong(editLog.getTxId());
            flushBuffer.writeByte(editLog.getOperation().value);
            flushBuffer.writeInt(editLog.getKey().length());
            flushBuffer.writeInt(editLog.getValue() == null ? 0 : editLog.getValue().length);
            flushBuffer.writeBytes(StringUtil.getBytes(editLog.getKey()));
            flushBuffer.writeBytes(editLog.getValue());
        }
    }

    public void flush(){

    }

    public ByteBuf readAll(){
        try{
            ByteBuf buffer = Unpooled.directBuffer();
            buffer.writeBytes(fileChannel, 0, (int)fileChannel.size());
            return buffer;
        }catch (IOException ignored){
            return null;
        }
    }


    public long minTxId(){
        return minTxId;
    }

    public long maxTxId(){
        return maxTxId;
    }
}
