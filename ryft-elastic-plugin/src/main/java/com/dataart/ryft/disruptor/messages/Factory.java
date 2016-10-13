package com.dataart.ryft.disruptor.messages;

import com.lmax.disruptor.EventFactory;

public class Factory<T> implements EventFactory<DisruptorEvent<T>> {
    @Override
    public DisruptorEvent newInstance() {
        return new DisruptorEvent();
    }
}