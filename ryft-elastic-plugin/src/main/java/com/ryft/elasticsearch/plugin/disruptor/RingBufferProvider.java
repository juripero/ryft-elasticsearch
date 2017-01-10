package com.ryft.elasticsearch.plugin.disruptor;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;

import com.ryft.elasticsearch.plugin.disruptor.messages.DisruptorEvent;
import com.ryft.elasticsearch.plugin.disruptor.messages.Factory;
import com.ryft.elasticsearch.plugin.elastic.plugin.PropertiesProvider;
import com.ryft.elasticsearch.plugin.elastic.plugin.RyftProperties;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

@Singleton
public class RingBufferProvider<T> implements Provider<RingBuffer<DisruptorEvent<T>>>, PostConstruct {
    Disruptor<DisruptorEvent<T>> disruptor;
    Set<EventHandler<DisruptorEvent<T>>> consumers;
    RyftProperties props;

    @Inject
    public RingBufferProvider(RyftProperties props, Set<EventHandler<DisruptorEvent<T>>> consumers) {
        this.props = props;
        this.consumers = consumers;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void onPostConstruct() {
        Executor executor = Executors.newCachedThreadPool();
        Factory<T> factory = new Factory<T>();
        disruptor = AccessController.doPrivileged((PrivilegedAction<Disruptor>) () -> {
            return new Disruptor<>(factory, props.getInt(PropertiesProvider.DISRUPTOR_CAPACITY), executor);
        });
        disruptor.handleEventsWith(consumers.toArray(new EventHandler[consumers.size()]));
        disruptor.start();
    }

    @Override
    public RingBuffer<DisruptorEvent<T>> get() {
        return disruptor.getRingBuffer();
    }

}
