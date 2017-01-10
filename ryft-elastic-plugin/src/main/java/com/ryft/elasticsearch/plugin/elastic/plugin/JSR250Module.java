package com.ryft.elasticsearch.plugin.elastic.plugin;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.common.inject.matcher.AbstractMatcher;
import org.elasticsearch.common.inject.matcher.Matcher;
import org.elasticsearch.common.inject.spi.InjectionListener;
import org.elasticsearch.common.inject.spi.TypeEncounter;
import org.elasticsearch.common.inject.spi.TypeListener;

import com.ryft.elasticsearch.plugin.disruptor.PostConstruct;

/**
 * Module responsible for binding postConstruct listeners;
 * Bind as first module in the module chain
 * 
 * @author imasternoy
 *
 */
public class JSR250Module extends AbstractModule {

    @Override
    protected void configure() {
        bindListener(POST_CONSTRUCT_MATCHER, new TypeListener() {
            @Override
            public <I> void hear(TypeLiteral<I> type, TypeEncounter<I> encounter) {
                try {
                    @SuppressWarnings("unchecked")
                    TypeEncounter<PostConstruct> g = (TypeEncounter<PostConstruct>) encounter;
                    g.register(InvokePostConstructMethod.INSTANCE);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
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
