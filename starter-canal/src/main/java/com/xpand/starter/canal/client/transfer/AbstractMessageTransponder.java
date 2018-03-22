package com.xpand.starter.canal.client.transfer;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.xpand.starter.canal.config.CanalConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract implements of the MessageTransponder interface.
 *
 * @author chen.qian
 * @date 2018/3/19
 */
public abstract class AbstractMessageTransponder implements MessageTransponder {

    /**
     * canal connector
     */
    private final CanalConnector connector;

    /**
     * custom config
     */
    protected final CanalConfig.Instance config;

    /**
     * destination of canal server
     */
    protected final String destination;

    /**
     * running flag
     */
    private volatile boolean running = true;

    private static final Logger logger = LoggerFactory.getLogger(AbstractMessageTransponder.class);

    public AbstractMessageTransponder(CanalConnector connector, String destination, CanalConfig.Instance config) {
        this.connector = connector;
        this.destination = destination;
        this.config = config;
    }

    @Override
    public void run() {
        int errorCount = config.getRetryCount();
        final long interval = config.getAcquireInterval();
        final String threadName = Thread.currentThread().getName();
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                Message message = connector.getWithoutAck(config.getBatchSize());
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (logger.isDebugEnabled()) {
                    logger.debug("{}: Get message from canal server >>>>> size:{}", threadName, size);
                }
                //empty message
                if (batchId == -1 || size == 0) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("{}: Empty message... sleep for {} millis", threadName, interval);
                    }
                    Thread.sleep(interval);
                } else {
                    distributeEvent(message);
                }
                // commit ack
                connector.ack(batchId);
                if (logger.isDebugEnabled()) {
                    logger.debug("{}: Ack message. batchId:", threadName, batchId);
                }
            } catch (CanalClientException e) {
                errorCount--;
                logger.error(threadName + ": Error occurred!! ", e);
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e1) {
                    errorCount = 0;
                }
            } catch (InterruptedException e) {
                errorCount = 0;
                connector.rollback();
            } finally {
                if (errorCount <= 0) {
                    stop();
                    logger.info("{}: Topping the client.. ", Thread.currentThread().getName());
                }
            }
        }
        stop();
        logger.info("{}: client stopped. ", Thread.currentThread().getName());
    }

    /**
     * to distribute the message to special event and let the event listeners to handle it
     *
     * @param message canal message
     */
    protected abstract void distributeEvent(Message message);

    /**
     * stop running
     */
    void stop() {
        running = false;
    }

}
