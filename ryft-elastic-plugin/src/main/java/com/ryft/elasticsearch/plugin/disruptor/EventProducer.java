package com.ryft.elasticsearch.plugin.disruptor;

import com.ryft.elasticsearch.plugin.disruptor.messages.RequestEvent;

public interface EventProducer<T extends RequestEvent> {

    void send(T t);
}
