package com.xpand.starter.canal.config;


import com.xpand.starter.canal.client.CanalClient;
import com.xpand.starter.canal.client.SimpleCanalClient;
import com.xpand.starter.canal.util.BeanUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;


/**
 * @author chen.qian
 * @date 2018/3/19
 */
public class CanalClientConfiguration {

    @Autowired
    private CanalConfig canalConfig;

    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    public BeanUtil beanUtil() {
        return new BeanUtil();
    }

    @Bean
    private CanalClient canalClient() {
        CanalClient canalClient = new SimpleCanalClient(canalConfig);
        canalClient.start();
        System.err.println("canal client started!");
        return canalClient;
    }
}
