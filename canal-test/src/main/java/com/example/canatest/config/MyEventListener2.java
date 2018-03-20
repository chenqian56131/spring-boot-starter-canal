package com.example.canatest.config;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.xpand.starter.canal.event.CanalEventListener;
import org.springframework.stereotype.Component;

/**
 * @author chen.qian
 * @date 2018/3/19
 */
@Component
public class MyEventListener2 implements CanalEventListener {
    @Override
    public void onEvent(CanalEntry.EventType eventType, CanalEntry.RowData rowData) {
        rowData.getAfterColumnsList().forEach((c) -> System.err.println("By--implements :" + c.getName() + " ::   " + c.getValue()));
    }
}
