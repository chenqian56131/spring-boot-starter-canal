package com.xpand.starter.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.xpand.starter.canal.client.exception.CanalClientException;
import com.xpand.starter.canal.config.CanalConfig;
import com.xpand.starter.canal.util.BeanUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Abstract implements of the CanalClient interface
 * It help to initialize the canal connector and so on..
 *
 * @author chen.qian
 * @date 2018/3/16
 */
public abstract class AbstractCanalClient implements CanalClient {

    /**
     * running flag
     */
    private volatile boolean running;

    /**
     * customer config
     */
    private CanalConfig canalConfig;

    AbstractCanalClient(CanalConfig canalConfig) {
        this.canalConfig = canalConfig;
    }

    @Override
    public void start() {
        Map<String, CanalConfig.Instance> instanceMap = getConfig();
        for (Map.Entry<String, CanalConfig.Instance> instanceEntry : instanceMap.entrySet()) {
            process(processInstanceEntry(instanceEntry), instanceEntry.getKey(), instanceEntry.getValue());
        }

    }

    /**
     * To initialize the canal connector
     * @param connector CanalConnector
     * @param destination destination
     * @param config config
     */
    protected abstract void process(CanalConnector connector, String destination, CanalConfig.Instance config);

    private CanalConnector processInstanceEntry(Map.Entry<String, CanalConfig.Instance> instanceEntry) {
        CanalConfig.Instance instance = instanceEntry.getValue();
        //use singleConnector
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(instance.getHost(), instance.getPort()),
                instanceEntry.getKey(),
                instance.getUserName(),
                instance.getPassword());
        connector.connect();
        if (!StringUtils.isEmpty(instance.getFilter())) {
            connector.subscribe(instance.getFilter());
        }
        connector.rollback();
        return connector;
    }

    /**
     * get the config
     *
     * @return config
     */
    protected Map<String, CanalConfig.Instance> getConfig() {
        CanalConfig config = canalConfig;
        Map<String, CanalConfig.Instance> instanceMap;
        if (config != null && (instanceMap = config.getInstances()) != null && !instanceMap.isEmpty()) {
            return config.getInstances();
        } else {
            throw new CanalClientException("can not get the configuration of canal client!");
        }
    }

    @Override
    public void stop() {
        setRunning(false);
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    protected void setRunning(boolean running) {
        this.running = running;
    }
}
