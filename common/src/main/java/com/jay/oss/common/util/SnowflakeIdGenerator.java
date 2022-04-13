package com.jay.oss.common.util;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/04/13 11:52
 */
public class SnowflakeIdGenerator {

    /**
     * ID中41位时间戳的起点 (2020-01-01 00:00:00.00)
     * @apiNote 一般地，选用系统上线的时间
     */
    private static final long START_POINT = 1577808000000L;

    /**
     * 序列号位数
     */
    private static final long SEQUENCE_BITS = 12L;

    /**
     * 机器ID位数
     */
    private static final long WORKER_ID_BITS = 5L;

    /**
     * 数据中心ID位数
     */
    private static final long DATA_CENTER_ID_BITS = 5L;

    /**
     * 序列号最大值, 4095
     * @apiNote 4095 = 0xFFF,其相当于是序列号掩码
     */
    private static final long SEQUENCE_MASK = ~(-1L << SEQUENCE_BITS);

    /**
     * 机器ID最大值, 31
     */
    private static final long MAX_WORKER_ID = ~(-1L << WORKER_ID_BITS);

    /**
     * 数据中心ID最大值, 31
     */
    private static final long MAX_DATA_CENTER_ID = ~(-1L << DATA_CENTER_ID_BITS);

    /**
     * 机器ID左移位数, 12
     */
    private static final long WORKER_ID_SHIFT = SEQUENCE_BITS;

    /**
     * 数据中心ID左移位数, 12+5
     */
    private static final long DATA_CENTER_ID_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;

    /**
     * 时间戳左移位数, 12+5+5
     */
    private static final long TIME_STAMP_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + DATA_CENTER_ID_BITS;

    /**
     * 数据中心ID, Value Range: [0,31]
     */
    private final long dataCenterId;

    /**
     * 机器ID, Value Range: [0,31]
     */
    private final long workerId;

    /**
     * 相同毫秒内的序列号, Value Range: [0,4095]
     */
    private long sequence = 0L;

    /**
     * 上一个生成ID的时间戳
     */
    private long lastTimeStamp = -1L;

    /**
     * 构造器
     * @param dataCenterId  数据中心ID
     * @param workerId 机器中心ID
     */
    public SnowflakeIdGenerator(Long dataCenterId, Long workerId) {
        if(dataCenterId==null || dataCenterId<0 || dataCenterId>MAX_DATA_CENTER_ID
                || workerId==null || workerId<0 || workerId>MAX_WORKER_ID) {
            throw new IllegalArgumentException("Wrong Argument");
        }
        this.dataCenterId = dataCenterId;
        this.workerId = workerId;
    }

    /**
     * 获取ID
     * @return long
     */
    public synchronized long nextId() {
        long currentTimeStamp = System.currentTimeMillis();
        //当前时间小于上一次生成ID的时间戳，系统时钟被回拨
        if( currentTimeStamp < lastTimeStamp ) {
            throw new RuntimeException("系统时钟被回拨");
        }

        // 当前时间等于上一次生成ID的时间戳,则通过序列号来区分
        if( currentTimeStamp == lastTimeStamp ) {
            // 通过序列号掩码实现只取 (sequence+1) 的低12位结果，其余位全部清零
            sequence = (sequence + 1) & SEQUENCE_MASK;
            if(sequence == 0) {
                // 阻塞等待下一个毫秒,并获取新的时间戳
                currentTimeStamp = getNextMs(lastTimeStamp);
            }
        } else {    // 当前时间大于上一次生成ID的时间戳,重置序列号
            sequence = 0;
        }

        // 更新上次时间戳信息
        lastTimeStamp = currentTimeStamp;

        // 生成此次ID
        return ((currentTimeStamp-START_POINT) << TIME_STAMP_SHIFT)
                | (dataCenterId << DATA_CENTER_ID_SHIFT)
                | (workerId << WORKER_ID_SHIFT)
                | sequence;
    }

    /**
     * 阻塞等待,直到获取新的时间戳(下一个毫秒)
     * @param lastTimeStamp lastTimeStamp
     * @return long nextTimeStamp
     */
    private long getNextMs(long lastTimeStamp) {
        long timeStamp = System.currentTimeMillis();
        while(timeStamp<=lastTimeStamp) {
            timeStamp = System.currentTimeMillis();
        }
        return timeStamp;
    }
}
