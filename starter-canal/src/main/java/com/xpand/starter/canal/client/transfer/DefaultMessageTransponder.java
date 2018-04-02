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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * @author chen.qian
 * @date 2018/3/23
 */
public class DefaultMessageTransponder extends AbstractBasicMessageTransponder {

    private final static Logger logger = LoggerFactory.getLogger(DefaultMessageTransponder.class);

    public DefaultMessageTransponder(CanalConnector connector,
                          Map.Entry<String, CanalConfig.Instance> config,
                          List<CanalEventListener> listeners,
                          List<ListenerPoint> annoListeners) {
        super(connector, config, listeners, annoListeners);
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
    @Override
    protected Predicate<Map.Entry<Method, ListenPoint>> getAnnotationFilter(String destination,
                                                                            String schemaName,
                                                                            String tableName,
                                                                            CanalEntry.EventType eventType) {
        Predicate<Map.Entry<Method, ListenPoint>> df = e -> StringUtils.isEmpty(e.getValue().destination())
                || e.getValue().destination().equals(destination);
        Predicate<Map.Entry<Method, ListenPoint>> sf = e -> e.getValue().schema().length == 0
                || Arrays.stream(e.getValue().schema()).anyMatch(s -> s.equals(schemaName));
        Predicate<Map.Entry<Method, ListenPoint>> tf = e -> e.getValue().table().length == 0
                || Arrays.stream(e.getValue().table()).anyMatch(t -> t.equals(tableName));
        Predicate<Map.Entry<Method, ListenPoint>> ef = e -> e.getValue().eventType().length == 0
                || Arrays.stream(e.getValue().eventType()).anyMatch(ev -> ev == eventType);
        return df.and(sf).and(tf).and(ef);
    }

    @Override
    protected Object[] getInvokeArgs(Method method, CanalEntry.EventType eventType, CanalEntry.RowData rowData) {
        return Arrays.stream(method.getParameterTypes())
                .map(p -> p == CanalEntry.EventType.class
                        ? eventType
                        : p == CanalEntry.RowData.class
                        ? rowData : null)
                .toArray();
    }

    @Override
    protected List<CanalEntry.EntryType> getIgnoreEntryTypes() {
        return Arrays.asList(CanalEntry.EntryType.TRANSACTIONBEGIN, CanalEntry.EntryType.TRANSACTIONEND, CanalEntry.EntryType.HEARTBEAT);
    }
}
