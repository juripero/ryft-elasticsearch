package com.ryft.elasticsearch.plugin.disruptor;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import com.ryft.elasticsearch.plugin.disruptor.messages.DisruptorEvent;
import com.ryft.elasticsearch.plugin.disruptor.messages.RequestEvent;
import com.lmax.disruptor.RingBuffer;

@Singleton
public class InternalEventProducer<T extends RequestEvent> implements EventProducer<T> {

    RingBuffer<DisruptorEvent<RequestEvent>> ringBuffer;
    @Inject
    public InternalEventProducer(RingBuffer<DisruptorEvent<RequestEvent>> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    @Override
    public void send(T t) {
        long sequence = ringBuffer.next();
        try {
            DisruptorEvent<RequestEvent> event = ringBuffer.get(sequence);
            event.setEvent(t);
        } finally {
            ringBuffer.publish(sequence);
        }
    }
}