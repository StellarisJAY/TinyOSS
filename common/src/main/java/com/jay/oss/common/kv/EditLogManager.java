package com.jay.oss.common.kv;

import com.jay.oss.common.util.Scheduler;
import com.jay.oss.common.util.ThreadPoolUtil;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * <p>
 *  EditLog 管理器
 *  负责管理EditLog和EditLog刷盘
 *  同时负责处理Backup Tracker发送的读取EditLog请求
 * </p>
 *
 * @author Jay
 * @date 2022/05/06 14:03
 */
public class EditLogManager {
    /**
     * 日志缓冲区
     * 追加编辑日志默认写入到缓冲区中，避免写文件
     */
    private final List<EditLog> editLogBuffer = new ArrayList<>();
    /**
     * 日志文件列表，缓冲区打满后写入文件后的日志
     */
    private final List<EditLogFile> editLogFiles = new ArrayList<>();

    private final AtomicLong nextTxId = new AtomicLong(0);
    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final ExecutorService asyncFlushExecutor = ThreadPoolUtil.newSingleThreadPool("edit-log-flush-");
    private long minBufferedTxId = Long.MIN_VALUE;

    /**
     * 刷盘间隔时间
     */
    private static final long EDIT_LOG_FLUSH_INTERVAL = 10 * 1000;
    /**
     * 刷盘阈值
     */
    private static final int FLUSH_THRESHOLD = 100;

    public void init(){
        // 定时刷盘
        Scheduler.scheduleAtFixedRate(this::flushEditLog, EDIT_LOG_FLUSH_INTERVAL, EDIT_LOG_FLUSH_INTERVAL, TimeUnit.MILLISECONDS);
    }

    /**
     * 追加EditLog
     * @param editLog {@link EditLog}
     */
    public void append(EditLog editLog){
        try{
            readWriteLock.writeLock().lock();
            // 获取事务ID
            editLog.setTxId(nextTxId.getAndIncrement());
            editLogBuffer.add(editLog);
            if(editLog.getTxId() < minBufferedTxId){
                minBufferedTxId = editLog.getTxId();
            }
            // 判断是否需要提交刷盘
            if(editLogBuffer.size() >= FLUSH_THRESHOLD){
                flushEditLog();
            }
        }finally {
            readWriteLock.writeLock().unlock();
        }
    }

    /**
     * Backup Tracker读取EditLog
     * @param txId 初始事务ID
     * @return {@link ByteBuf}
     */
    public ByteBuf fetchEditLogs(long txId){
        // 判断事务ID是否在当前缓冲区中
        if(txId >= minBufferedTxId){
            try{
                readWriteLock.readLock().lock();
                return null;
            }finally {
                readWriteLock.readLock().unlock();
            }
        }else{
            // 不在缓冲区中，从EditLog文件读取
            List<EditLogFile> targetLogFiles = editLogFiles.stream()
                    .filter((editLogFile -> editLogFile.minTxId() >= txId))
                    .collect(Collectors.toList());
            return targetLogFiles.get(0).readAll();
        }
    }

    /**
     * 提交EditLog刷盘
     */
    private void flushEditLog(){
        try{
            readWriteLock.writeLock().lock();
            if(editLogBuffer.size() >= FLUSH_THRESHOLD){
                // 将缓冲区的editLog按照txID排序
                List<EditLog> sortedEditLogs = editLogBuffer.stream()
                        .sorted((o1, o2) -> (int) (o2.getTxId() - o1.getTxId()))
                        .collect(Collectors.toList());
                // 创建新的EditLog文件
                EditLogFile editLogFile = new EditLogFile(sortedEditLogs);
                editLogFiles.add(editLogFile);
                // 清空缓冲区
                editLogBuffer.clear();
                // 提交异步刷盘任务
                asyncFlushExecutor.submit(editLogFile::flush);
            }
        }finally {
            readWriteLock.writeLock().unlock();
        }
    }


}
