package com.jay.oss.common.util;

import com.jay.dove.util.NamedThreadFactory;

import java.util.concurrent.*;

/**
 * <p>
 *  线程池工具，提供线程池
 * </p>
 *
 * @author Jay
 * @date 2022/02/12 13:10
 */
public class ThreadPoolUtil {
    public static ExecutorService newSingleThreadPool(String name){
        return new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(), new NamedThreadFactory(name, true));
    }

    public static ExecutorService newThreadPool(int core, int max, String name){
        return new ThreadPoolExecutor(core, max,
                0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new NamedThreadFactory(name, true));
    }

    /**
     * 新建IO密集型任务线程池
     * @param name 线程名
     * @return {@link ExecutorService}
     */
    public static ExecutorService newIoThreadPool(String name){
        return newThreadPool(2 * Runtime.getRuntime().availableProcessors(),
                2 * Runtime.getRuntime().availableProcessors(), name);
    }
}
