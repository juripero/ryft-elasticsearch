package com.ryft.elasticsearch.plugin.disruptor;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.common.inject.multibindings.Multibinder;

import com.ryft.elasticsearch.plugin.disruptor.messages.DisruptorEvent;
import com.ryft.elasticsearch.plugin.disruptor.messages.InternalEvent;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.ryft.elasticsearch.plugin.disruptor.messages.RyftClusterRequestEvent;
import com.ryft.elasticsearch.plugin.disruptor.messages.RyftClusterRequestEventFactory;
import com.ryft.elasticsearch.plugin.elastic.plugin.cluster.RyftClusterService;
import com.ryft.elasticsearch.plugin.elastic.plugin.cluster.RyftClusterServiceFactory;
import org.elasticsearch.common.inject.assistedinject.FactoryProvider;

public class DisruptorMessageBusModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(new TypeLiteral<EventProducer<RyftClusterRequestEvent>>() {
        }).to(new TypeLiteral<InternalEventProducer<RyftClusterRequestEvent>>() {
        });

        Multibinder<EventHandler<DisruptorEvent<InternalEvent>>> binder = Multibinder.newSetBinder(binder(),
                new TypeLiteral<EventHandler<DisruptorEvent<InternalEvent>>>() {
        });

        binder.addBinding().to(new TypeLiteral<RyftRequestEventConsumer>() {
        }).asEagerSingleton();

        bind(new TypeLiteral<RingBuffer<DisruptorEvent<InternalEvent>>>() {
        }).toProvider(Key.get(new TypeLiteral<RingBufferProvider<InternalEvent>>() {
        })).in(Singleton.class);

        bind(RyftClusterRequestEventFactory.class).toProvider(
                FactoryProvider.newFactory(RyftClusterRequestEventFactory.class, RyftClusterRequestEvent.class)).in(Singleton.class);

        bind(RyftClusterServiceFactory.class).toProvider(
                FactoryProvider.newFactory(RyftClusterServiceFactory.class, RyftClusterService.class)).in(Singleton.class);
    }

}
