package com.xpj;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.scheduling.annotation.Async;

public abstract class AbstractCanalClient implements CommandLineRunner {

    @Autowired
    private CanalConnector connector;

    protected volatile boolean running = true;

    protected final static Logger logger = LoggerFactory.getLogger(AbstractCanalClient.class);


    @Async
    @Override
    public void run(String... args){
        int batchSize = 5 * 1024;
        while (running) {
            try {
                connector.connect();
                connector.subscribe();
                while (running) {
                    Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    if (batchId == -1 || size == 0) {
                        //没有内容变化时
                    } else {
                        try {
                            handle(message);
                            connector.ack(batchId); // 提交确认
                        }catch (Exception e){
                            connector.rollback(batchId);
                            throw e;
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("process error!", e);
            } finally {
                connector.disconnect();
            }
        }
    }


    protected void stop() {
        if (!running) {
            return;
        }
        running = false;
    }

    protected abstract void handle(Message message);
}
