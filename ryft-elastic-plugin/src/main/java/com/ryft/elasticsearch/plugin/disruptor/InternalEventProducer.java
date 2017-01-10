package com.ryft.elasticsearch.plugin.disruptor;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import com.ryft.elasticsearch.plugin.disruptor.messages.DisruptorEvent;
import com.ryft.elasticsearch.plugin.disruptor.messages.InternalEvent;
import com.lmax.disruptor.RingBuffer;

@Singleton
public class InternalEventProducer<T extends InternalEvent> implements EventProducer<T> {

    RingBuffer<DisruptorEvent<InternalEvent>> ringBuffer;
    @Inject
    public InternalEventProducer(RingBuffer<DisruptorEvent<InternalEvent>> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    @Override
    public void send(T t) {
        long sequence = ringBuffer.next();
        try {
            DisruptorEvent<InternalEvent> event = ringBuffer.get(sequence);
            event.setEvent(t);
        } finally {
            ringBuffer.publish(sequence);
        }
    }
}