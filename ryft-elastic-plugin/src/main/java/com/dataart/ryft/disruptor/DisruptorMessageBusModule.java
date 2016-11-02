package com.dataart.ryft.disruptor;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.common.inject.multibindings.Multibinder;

import com.dataart.ryft.disruptor.messages.DisruptorEvent;
import com.dataart.ryft.disruptor.messages.InternalEvent;
import com.dataart.ryft.disruptor.messages.RyftRequestEvent;
import com.dataart.ryft.disruptor.messages.RyftRequestEventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import org.elasticsearch.common.inject.assistedinject.FactoryProvider;

public class DisruptorMessageBusModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(new TypeLiteral<EventProducer<RyftRequestEvent>>() {
        }).to(new TypeLiteral<InternalEventProducer<RyftRequestEvent>>() {
        });

        Multibinder<EventHandler<DisruptorEvent<InternalEvent>>> binder = Multibinder.newSetBinder(binder(),
                new TypeLiteral<EventHandler<DisruptorEvent<InternalEvent>>>() {
        });

        binder.addBinding().to(new TypeLiteral<RyftRequestEventConsumer>() {
        }).asEagerSingleton();

        bind(new TypeLiteral<RingBuffer<DisruptorEvent<InternalEvent>>>() {
        }).toProvider(Key.get(new TypeLiteral<RingBufferProvider<InternalEvent>>() {
        })).in(Singleton.class);

        bind(RyftRequestEventFactory.class).toProvider(
                FactoryProvider.newFactory(RyftRequestEventFactory.class, RyftRequestEvent.class)).in(Singleton.class);
    }

}
