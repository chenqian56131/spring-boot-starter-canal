package com.xpand.starter.canal.client.transfer;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.xpand.starter.canal.annotation.ListenPoint;
import com.xpand.starter.canal.client.ListenerPoint;
import com.xpand.starter.canal.client.exception.CanalClientException;
import com.xpand.starter.canal.config.CanalConfig;
import com.xpand.starter.canal.event.CanalEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * @author chen.qian
 * @date 2018/4/2
 */
public abstract class AbstractBasicMessageTransponder extends AbstractMessageTransponder {

    private final static Logger logger = LoggerFactory.getLogger(AbstractBasicMessageTransponder.class);

    public AbstractBasicMessageTransponder(CanalConnector connector, Map.Entry<String, CanalConfig.Instance> config, List<CanalEventListener> listeners, List<ListenerPoint> annoListeners) {
        super(connector, config, listeners, annoListeners);
    }

    @Override
    protected void distributeEvent(Message message) {
        List<CanalEntry.Entry> entries = message.getEntries();
        for (CanalEntry.Entry entry : entries) {
            //ignore the transaction operations
            List<CanalEntry.EntryType> ignoreEntryTypes = getIgnoreEntryTypes();
            if (ignoreEntryTypes != null
                    && ignoreEntryTypes.stream().anyMatch(t -> entry.getEntryType() == t)) {
                continue;
            }
            CanalEntry.RowChange rowChange;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new CanalClientException("ERROR ## parser of event has an error , data:" + entry.toString(),
                        e);
            }
            //ignore the ddl operation
            if (rowChange.hasIsDdl() && rowChange.getIsDdl()) {
                processDdl(rowChange);
                continue;
            }
            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                //distribute to listener interfaces
                distributeByImpl(rowChange.getEventType(), rowData);
                //distribute to annotation listener interfaces
                distributeByAnnotation(destination,
                        entry.getHeader().getSchemaName(),
                        entry.getHeader().getTableName(),
                        rowChange.getEventType(),
                        rowData);
            }
        }
    }

    /**
     * process the ddl event
     * @param rowChange rowChange
     */
    protected void processDdl(CanalEntry.RowChange rowChange) {}

    /**
     * distribute to listener interfaces
     *
     * @param eventType eventType
     * @param rowData rowData
     */
    protected void distributeByImpl(CanalEntry.EventType eventType, CanalEntry.RowData rowData) {
        if (listeners != null) {
            for (CanalEventListener listener : listeners) {
                listener.onEvent(eventType, rowData);
            }
        }
    }

    /**
     * distribute to annotation listener interfaces
     *
     * @param destination destination
     * @param schemaName schema
     * @param tableName table name
     * @param eventType event type
     * @param rowData row data
     */
    protected void distributeByAnnotation(String destination,
                                        String schemaName,
                                        String tableName,
                                        CanalEntry.EventType eventType,
                                        CanalEntry.RowData rowData) {
        //invoke the listeners
        annoListeners.forEach(point -> point
                .getInvokeMap()
                .entrySet()
                .stream()
                .filter(getAnnotationFilter(destination, schemaName, tableName, eventType))
                .forEach(entry -> {
                    Method method = entry.getKey();
                    method.setAccessible(true);
                    try {
                        Object[] args = getInvokeArgs(method, eventType, rowData);
                        method.invoke(point.getTarget(), args);
                    } catch (Exception e) {
                        logger.error("{}: Error occurred when invoke the listener's interface! class:{}, method:{}",
                                Thread.currentThread().getName(),
                                point.getTarget().getClass().getName(), method.getName());
                    }
                }));
    }

    /**
     * get the filters predicate
     *
     * @param destination destination
     * @param schemaName schema
     * @param tableName table name
     * @param eventType event type
     * @return predicate
     */
    protected abstract Predicate<Map.Entry<Method, ListenPoint>> getAnnotationFilter(String destination,
                                                                            String schemaName,
                                                                            String tableName,
                                                                            CanalEntry.EventType eventType);

    /**
     * get the args
     *
     * @param method method
     * @param eventType event type
     * @param rowData row data
     * @return args which will be used by invoking the annotation methods
     */
    protected abstract Object[] getInvokeArgs(Method method, CanalEntry.EventType eventType,
                                              CanalEntry.RowData rowData);

    /**
     * get the ignore eventType list
     *
     * @return eventType list
     */
    protected List<CanalEntry.EntryType> getIgnoreEntryTypes() {
        return Collections.emptyList();
    }

}
