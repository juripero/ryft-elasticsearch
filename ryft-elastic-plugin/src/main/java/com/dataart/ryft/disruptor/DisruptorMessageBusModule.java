package com.dataart.ryft.disruptor;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.common.inject.matcher.AbstractMatcher;
import org.elasticsearch.common.inject.matcher.Matcher;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.common.inject.spi.InjectionListener;
import org.elasticsearch.common.inject.spi.TypeEncounter;
import org.elasticsearch.common.inject.spi.TypeListener;

import com.dataart.ryft.disruptor.messages.DisruptorEvent;
import com.dataart.ryft.disruptor.messages.InternalEvent;
import com.dataart.ryft.disruptor.messages.RyftRequestEvent;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;

public class DisruptorMessageBusModule extends AbstractModule {

    @Override
    protected void configure() {
        bindListener(POST_CONSTRUCT_MATCHER, new TypeListener() {
            @Override
            public <I> void hear(TypeLiteral<I> type, TypeEncounter<I> encounter) {
                try {
                    TypeEncounter<PostConstruct> g = (TypeEncounter<PostConstruct>) encounter;
                    g.register(InvokePostConstructMethod.INSTANCE);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });

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

    }

    /**
     * PostConstruct developed according to
     * http://developer.vz.net/2012/02/08/extending-guice-2/
     */
    private static final Matcher<TypeLiteral<?>> POST_CONSTRUCT_MATCHER = new TypeMatcher(PostConstruct.class);

    private static final class TypeMatcher extends AbstractMatcher<TypeLiteral<?>> {
        private final Class<?> type;

        public TypeMatcher(Class<?> type) {
            this.type = type;
        }

        public boolean matches(TypeLiteral<?> type) {
            try {
                return this.type.isAssignableFrom(type.getRawType());
            } catch (Exception e) {
                return false;
            }
        }
    }

    /**
     * invoke the afterPropertiesSet() method in class that guice is
     * constructing
     */
    static enum InvokePostConstructMethod implements InjectionListener<PostConstruct> {
        INSTANCE;

        public void afterInjection(PostConstruct injectee) {
            try {
                PostConstruct g = (PostConstruct) injectee;
                g.onPostConstruct();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

}
