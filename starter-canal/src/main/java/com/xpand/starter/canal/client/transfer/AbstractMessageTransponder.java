package com.xpand.starter.canal.client.transfer;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import com.xpand.starter.canal.config.CanalConfig;

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

    public AbstractMessageTransponder(CanalConnector connector, String destination, CanalConfig.Instance config) {
        this.connector = connector;
        this.destination = destination;
        this.config = config;
    }

    @Override
    public void run() {
        while (running && !Thread.currentThread().isInterrupted()) {
            Message message = connector.getWithoutAck(config.getBatchSize());
            long batchId = message.getId();
            int size = message.getEntries().size();
            //empty message
            if (batchId == -1 || size == 0) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    stop();
                }
            } else {
                distributeEvent(message);
            }
            // commit ack
            connector.ack(batchId);
        }
        stop();
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
