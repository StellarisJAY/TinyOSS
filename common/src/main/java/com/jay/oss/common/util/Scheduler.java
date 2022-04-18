package com.jay.oss.common.util;

import com.jay.dove.util.NamedThreadFactory;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/02/12 13:18
 */
public class Scheduler {
    private static final ScheduledThreadPoolExecutor SCHEDULER = new ScheduledThreadPoolExecutor(3, new NamedThreadFactory("scheduler-"));

    public static void scheduleAtFixedRate(Runnable task, long delay, long period, TimeUnit timeUnit){
        SCHEDULER.scheduleAtFixedRate(task, delay, period, timeUnit);
    }

    public static void scheduleAtFixedMinutes(Runnable task, int delay, int period){
        SCHEDULER.scheduleAtFixedRate(task, delay, period, TimeUnit.MINUTES);
    }
}
