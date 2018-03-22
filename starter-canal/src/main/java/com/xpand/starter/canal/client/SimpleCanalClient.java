package com.xpand.starter.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.xpand.starter.canal.annotation.ListenPoint;
import com.xpand.starter.canal.client.exception.CanalClientException;
import com.xpand.starter.canal.client.transfer.AbstractMessageTransponder;
import com.xpand.starter.canal.config.CanalConfig;
import com.xpand.starter.canal.event.CanalEventListener;
import com.xpand.starter.canal.util.BeanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.StringUtils;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * SimpleCanalClient
 * use cache thread pool to handle the event
 *
 * @author chen.qian
 * @date 2018/3/16
 */
public class SimpleCanalClient extends AbstractCanalClient {

    /**
     * executor
     */
    private ThreadPoolExecutor executor;

    private final static Logger logger = LoggerFactory.getLogger(SimpleCanalClient.class);

    public SimpleCanalClient(CanalConfig canalConfig) {
        super(canalConfig);
        executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<>(), Executors.defaultThreadFactory());
    }

    @Override
    protected void process(CanalConnector connector, String destination, CanalConfig.Instance config) {
        executor.submit(new MsgTransponder(connector, destination, config));
    }

    @Override
    public void stop() {
        super.stop();
        executor.shutdown();
    }

    /**
     * Transponder implements
     */
    private static class MsgTransponder extends AbstractMessageTransponder {

        /**
         * listeners which are used by implementing the Interface
         */
        private final List<CanalEventListener> listeners = new ArrayList<>();

        /**
         * listeners which are used by annotation
         */
        private final List<ListenerPoint> annoListeners = new ArrayList<>();

        MsgTransponder(CanalConnector connector, String destination, CanalConfig.Instance config) {
            super(connector, destination, config);
            //initialize the listeners
            logger.info("{}: initializing the listeners....", Thread.currentThread().getName());
            List<CanalEventListener> list = BeanUtil.getBeansOfType(CanalEventListener.class);
            if (list != null) {
                listeners.addAll(list);
            }
            Map<String, Object> listenerMap = BeanUtil.getBeansWithAnnotation(com.xpand.starter.canal.annotation.CanalEventListener.class);
            if (listenerMap != null) {
                for (Object target : listenerMap.values()) {
                    Method[] methods = target.getClass().getDeclaredMethods();
                    if (methods != null && methods.length > 0) {
                        for (Method method : methods) {
                            ListenPoint l = AnnotationUtils.findAnnotation(method, ListenPoint.class);
                            if (l != null) {
                                annoListeners.add(new ListenerPoint(target, method, l));
                            }
                        }
                    }
                }
            }
            logger.info("{}: initializing the listeners end.", Thread.currentThread().getName());
            if (logger.isWarnEnabled() && listeners.isEmpty() && annoListeners.isEmpty()) {
                logger.warn("{}: No listener found in context! ", Thread.currentThread().getName());
            }
        }

        /**
         * ListenerPoint
         * save the information of listener's method-info
         */
        private static class ListenerPoint {
            Object target;
            Map<Method, ListenPoint> invokeMap = new HashMap<>();

            ListenerPoint(Object target, Method method, ListenPoint anno) {
                this.target = target;
                this.invokeMap.put(method, anno);
            }
        }

        @Override
        protected void distributeEvent(Message message) {
            List<CanalEntry.Entry> entries = message.getEntries();
            for (CanalEntry.Entry entry : entries) {
                //ignore the transaction operations
                if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                    continue;
                }
                CanalEntry.RowChange rowChange;
                try {
                    rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new CanalClientException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                            e);
                }
                //ignore the ddl operation
                if (rowChange.hasIsDdl() && rowChange.getIsDdl()) {
                    continue;
                }
                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                    //distribute to listener interfaces
                    if (listeners != null) {
                        for (CanalEventListener listener : listeners) {
                            listener.onEvent(rowChange.getEventType(), rowData);
                        }
                    }
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
            annoListeners.forEach(point -> point.invokeMap
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
                            method.invoke(point.target, args);
                        } catch (Exception e) {
                            logger.error("{}: Error occurred when invoke the listener's interface! class:{}, method:{}",
                                    Thread.currentThread().getName(),
                                    point.target.getClass().getName(), method.getName());
                        }
                    }));
        }
    }
}
