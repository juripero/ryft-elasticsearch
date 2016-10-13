package com.dataart.ryft.disruptor;

import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;


import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;

import com.dataart.ryft.disruptor.messages.DisruptorEvent;
import com.dataart.ryft.disruptor.messages.Factory;
import com.dataart.ryft.disruptor.messages.InternalEvent;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

@Singleton
public class RingBufferProvider<T> implements Provider<RingBuffer<DisruptorEvent<T>>>, PostConstruct {

    private static int DEFAULT_DISRUPTOR_CAPACITY = 1024 * 1024;

    Disruptor<DisruptorEvent<T>> disruptor;

    Set<EventHandler<DisruptorEvent<T>>> consumers;

    @Inject
    public RingBufferProvider(Set<EventHandler<DisruptorEvent<T>>> consumers) {
        this.consumers = consumers;
    }

    @Override
    public void onPostConstruct() {
        Executor executor = Executors.newCachedThreadPool();
        Factory<T> factory = new Factory<T>();
        disruptor = new Disruptor<>(factory, DEFAULT_DISRUPTOR_CAPACITY, executor);
        disruptor.handleEventsWith(consumers.toArray(new EventHandler[consumers.size()]));
        disruptor.start();
    }

    @Override
    public RingBuffer<DisruptorEvent<T>> get() {
        return disruptor.getRingBuffer();
    }

}
