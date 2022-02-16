package com.jay.oss.common.util;

/**
 * <p>
 *  APP id 64 位
 *  1 bit 符号位
 *  41 bit 时间戳
 *  22 bit 序列号
 * </p>
 *
 * @author Jay
 * @date 2022/02/16 10:57
 */
public class AppIdUtil {
    /**
     * 序列号
     */
    private static int sequence;

    private static final int SEQUENCE_BITS = 22;
    private static final int TIMESTAMP_LEFT = SEQUENCE_BITS;
    /**
     * 初始时间戳，2022/3/1 00：00：00
     */
    private static final long INITIAL_TIME = 1646064000000L;

    private static long lastTimestamp = System.currentTimeMillis();

    public static synchronized long getAppId(){
        long timestamp = System.currentTimeMillis();
        if(timestamp != lastTimestamp){
            sequence = 0;
        }else{
            lastTimestamp = timestamp;
            sequence++;
        }
        long value = timestamp - INITIAL_TIME << TIMESTAMP_LEFT
                | sequence;
        return Math.abs(value);
    }
}
