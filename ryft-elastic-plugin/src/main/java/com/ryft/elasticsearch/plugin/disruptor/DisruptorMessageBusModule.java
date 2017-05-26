package com.ryft.elasticsearch.plugin.disruptor;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.common.inject.multibindings.Multibinder;

import com.ryft.elasticsearch.plugin.disruptor.messages.DisruptorEvent;
import com.ryft.elasticsearch.plugin.disruptor.messages.RequestEvent;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.ryft.elasticsearch.plugin.disruptor.messages.FileSearchRequestEvent;
import com.ryft.elasticsearch.plugin.disruptor.messages.FileSearchRequestEventFactory;
import com.ryft.elasticsearch.plugin.disruptor.messages.IndexSearchRequestEvent;
import com.ryft.elasticsearch.plugin.elastic.plugin.cluster.RyftSearchService;
import com.ryft.elasticsearch.plugin.elastic.plugin.cluster.RyftClusterServiceFactory;
import org.elasticsearch.common.inject.assistedinject.FactoryProvider;
import com.ryft.elasticsearch.plugin.disruptor.messages.IndexSearchRequestEventFactory;

public class DisruptorMessageBusModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(new TypeLiteral<EventProducer<RequestEvent>>() {
        }).to(new TypeLiteral<InternalEventProducer<RequestEvent>>() {
        });

        Multibinder<EventHandler<DisruptorEvent<RequestEvent>>> binder = Multibinder.newSetBinder(binder(),
                new TypeLiteral<EventHandler<DisruptorEvent<RequestEvent>>>() {
        });

        binder.addBinding().to(new TypeLiteral<RyftRequestEventConsumer>() {
        }).asEagerSingleton();

        bind(new TypeLiteral<RingBuffer<DisruptorEvent<RequestEvent>>>() {
        }).toProvider(Key.get(new TypeLiteral<RingBufferProvider<RequestEvent>>() {
        })).in(Singleton.class);

        bind(IndexSearchRequestEventFactory.class).toProvider(
                FactoryProvider.newFactory(IndexSearchRequestEventFactory.class, IndexSearchRequestEvent.class)).in(Singleton.class);

        bind(FileSearchRequestEventFactory.class).toProvider(
                FactoryProvider.newFactory(FileSearchRequestEventFactory.class, FileSearchRequestEvent.class)).in(Singleton.class);

        bind(RyftClusterServiceFactory.class).toProvider(FactoryProvider.newFactory(RyftClusterServiceFactory.class, RyftSearchService.class)).in(Singleton.class);
    }

}
