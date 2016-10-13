package com.dataart.ryft.disruptor;

import com.dataart.ryft.disruptor.messages.InternalEvent;

public interface EventProducer<T extends InternalEvent> {
  void send(T t);
}