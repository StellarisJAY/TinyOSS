package com.jay.oss.common.prometheus;

import io.prometheus.client.Gauge;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 *  Prometheus Gauge 管理器
 * </p>
 *
 * @author Jay
 * @date 2022/03/24 12:09
 */
public class GaugeManager {
    private static final Map<String, Gauge> GAUGE_MAP = new HashMap<>();

    public static void registerGauge(String name, Gauge gauge){
        GAUGE_MAP.put(name, gauge);
        gauge.register();
    }

    public static Gauge getGauge(String name){
        return GAUGE_MAP.get(name);
    }
}
