package com.ryft.elasticsearch.plugin.disruptor;

import java.util.Map;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import com.ryft.elasticsearch.plugin.disruptor.messages.DisruptorEvent;
import com.ryft.elasticsearch.plugin.disruptor.messages.EventType;
import com.ryft.elasticsearch.plugin.disruptor.messages.RequestEvent;
import com.ryft.elasticsearch.rest.processors.RyftProcessor;
import com.lmax.disruptor.EventHandler;

@Singleton
public class RyftRequestEventConsumer implements EventHandler<DisruptorEvent<RequestEvent>> {
    private final ESLogger logger = Loggers.getLogger(getClass());

    Map<EventType, RyftProcessor> processors;

    @Inject
    public RyftRequestEventConsumer(Map<EventType, RyftProcessor> processors) {
        this.processors = processors;
    }

    @Override
    public void onEvent(DisruptorEvent<RequestEvent> event, long sequence, boolean endOfBatch) throws Exception {
        logger.info("Message consumed {}", event.getEvent().getEventType());
        processors.get(event.getEvent().getEventType()).process(event.getEvent());
    }
}
