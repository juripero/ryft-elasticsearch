package com.dataart.ryft.disruptor;

import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import com.dataart.ryft.disruptor.messages.DisruptorEvent;
import com.dataart.ryft.disruptor.messages.InternalEvent;
import com.lmax.disruptor.EventHandler;

@Singleton
public class RyftRequestEventConsumer implements EventHandler<DisruptorEvent<InternalEvent>> {
    private final ESLogger logger = Loggers.getLogger(getClass());

    @Override
    public void onEvent(DisruptorEvent<InternalEvent> event, long sequence, boolean endOfBatch) throws Exception {

        logger.info("Message consumed {}", event.getEvent());
    }
}
