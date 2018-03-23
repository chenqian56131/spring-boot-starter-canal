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
import org.springframework.util.StringUtils;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * @author chen.qian
 * @date 2018/3/23
 */
public class DefaultMessageTransponder extends AbstractMessageTransponder {

    private final static Logger logger = LoggerFactory.getLogger(DefaultMessageTransponder.class);

    public DefaultMessageTransponder(CanalConnector connector,
                          Map.Entry<String, CanalConfig.Instance> config,
                          List<CanalEventListener> listeners,
                          List<ListenerPoint> annoListeners) {
        super(connector, config, listeners, annoListeners);
    }

    @Override
    protected void distributeEvent(Message message) {
        List<CanalEntry.Entry> entries = message.getEntries();
        for (CanalEntry.Entry entry : entries) {
            //ignore the transaction operations
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                    || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND
                    || entry.getEntryType() == CanalEntry.EntryType.HEARTBEAT) {
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
     * distribute to listener interfaces
     *
     * @param eventType eventType
     * @param rowData rowData
     */
    private void distributeByImpl(CanalEntry.EventType eventType, CanalEntry.RowData rowData) {
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
    private void distributeByAnnotation(String destination,
                                        String schemaName,
                                        String tableName,
                                        CanalEntry.EventType eventType,
                                        CanalEntry.RowData rowData) {

        //filter which used to filter the event
        Predicate<Map.Entry<Method, ListenPoint>> filter = entry -> {
            ListenPoint l = entry.getValue();
            return l != null
                    && (StringUtils.isEmpty(l.destination()) || l.destination().equals(destination))
                    && (l.schema().length == 0 || Arrays.stream(l.schema()).anyMatch(s -> s.equals(schemaName)))
                    && (l.table().length == 0 || Arrays.stream(l.table()).anyMatch(t -> t.equals(tableName)))
                    && (l.eventType().length == 0 || Arrays.stream(l.eventType()).anyMatch(e -> e == eventType));
        };
        //invoke the listeners
        annoListeners.forEach(point -> point
                .getInvokeMap()
                .entrySet()
                .stream()
                .filter(filter)
                .forEach(entry -> {
                    Method method = entry.getKey();
                    method.setAccessible(true);
                    try {
                        Object[] args;
                        args = Arrays.stream(method.getParameterTypes())
                                .map(p -> p == CanalEntry.EventType.class
                                        ? eventType
                                        : p == CanalEntry.RowData.class
                                        ? rowData : null)
                                .toArray();
                        method.invoke(point.getTarget(), args);
                    } catch (Exception e) {
                        logger.error("{}: Error occurred when invoke the listener's interface! class:{}, method:{}",
                                Thread.currentThread().getName(),
                                point.getTarget().getClass().getName(), method.getName());
                    }
                }));
    }
}
