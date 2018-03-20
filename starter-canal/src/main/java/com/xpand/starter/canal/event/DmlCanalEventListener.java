package com.xpand.starter.canal.event;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.Objects;

/**
 * DmlCanalEventListener
 *
 * @author chen.qian
 * @date 2018/3/19
 */
public interface DmlCanalEventListener extends CanalEventListener {

    @Override
    default void onEvent(CanalEntry.EventType eventType, CanalEntry.RowData rowData) {
        Objects.requireNonNull(eventType);
        switch (eventType) {
            case INSERT:
                onInsert(rowData);
                break;
            case UPDATE:
                onUpdate(rowData);
                break;
            case DELETE:
                onDelete(rowData);
                break;
            default:
                break;
        }
    }

    /**
     * fired on insert event
     *
     * @param rowData rowData
     */
    void onInsert(CanalEntry.RowData rowData);

    /**
     * fired on update event
     *
     * @param rowData rowData
     */
    void onUpdate(CanalEntry.RowData rowData);

    /**
     * fired on delete event
     *
     * @param rowData rowData
     */
    void onDelete(CanalEntry.RowData rowData);

}
