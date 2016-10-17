package com.dataart.ryft.disruptor;

import java.util.Map;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import com.dataart.ryft.disruptor.messages.DisruptorEvent;
import com.dataart.ryft.disruptor.messages.EventType;
import com.dataart.ryft.disruptor.messages.InternalEvent;
import com.dataart.ryft.processors.RyftProcessor;
import com.lmax.disruptor.EventHandler;

@Singleton
public class RyftRequestEventConsumer implements EventHandler<DisruptorEvent<InternalEvent>> {
    private final ESLogger logger = Loggers.getLogger(getClass());

    Map<EventType, RyftProcessor> processors;

    @Inject
    public RyftRequestEventConsumer(Map<EventType, RyftProcessor> processors) {
        this.processors = processors;
    }

    @Override
    public void onEvent(DisruptorEvent<InternalEvent> event, long sequence, boolean endOfBatch) throws Exception {
        logger.info("Message consumed {}", event.getEvent().getEventType());
        processors.get(event.getEvent().getEventType()).process(event.getEvent());
    }
}
