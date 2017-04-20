package com.ryft.elasticsearch.plugin.disruptor;

import com.ryft.elasticsearch.plugin.disruptor.messages.InternalEvent;

public interface EventProducer<T extends InternalEvent> {
  void send(T t);
}