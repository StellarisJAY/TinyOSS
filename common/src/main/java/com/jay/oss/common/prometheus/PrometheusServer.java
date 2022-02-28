package com.jay.oss.common.prometheus;

import com.jay.dove.common.AbstractLifeCycle;
import com.jay.oss.common.config.OssConfigs;
import io.prometheus.client.Collector;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/02/28 14:50
 */
@Slf4j
public class PrometheusServer extends AbstractLifeCycle {

    private HTTPServer httpServer;
    private final List<Collector> collectors = new LinkedList<>();

    /**
     * 添加Collector
     * @param collector {@link Collector}
     */
    public void addCollector(Collector collector){
        collectors.add(collector);
    }

    private void init() throws Exception{
        // 注册collector
        for (Collector collector : collectors) {
            collector.register();
        }
        // 初始化JVM信息Exporter
        DefaultExports.initialize();
        // 启动Prometheus HTTP服务器
        this.httpServer = new HTTPServer(OssConfigs.prometheusServerPort(), true);
    }

    @Override
    public void startup() {
        super.startup();
        try{
            init();
        }catch (Exception e){
            log.error("prometheus server error ", e);
        }
    }

    @Override
    public void shutdown() {
        super.shutdown();
        httpServer.stop();
    }
}
