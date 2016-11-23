package com.dataart.ryft.elastic.plugin;

import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.inject.multibindings.MapBinder;

import com.dataart.ryft.disruptor.DisruptorMessageBusModule;
import com.dataart.ryft.elastic.converter.ElasticConversionModule;
import com.dataart.ryft.elastic.plugin.interceptors.ActionInterceptor;
import com.dataart.ryft.elastic.plugin.interceptors.IndexInterceptor;
import com.dataart.ryft.elastic.plugin.interceptors.SearchInterceptor;
import com.dataart.ryft.elastic.plugin.rest.client.RyftRestClient;
import com.dataart.ryft.processors.ProcessorsModule;

/**
 *
 * @author imasternoy
 */
public class RyftElasticModule extends AbstractModule {

    @Override
    protected void configure() {
        install(new JSR250Module());

        bind(RyftProperties.class).toProvider(PropertiesProvider.class).in(Singleton.class);
        // TODO: [imasternoy] Think about provider for this
        // bind(RyftRestClient.class);
        bind(RestSearchActionFilter.class);

        MapBinder<String, ActionInterceptor> interceptors = MapBinder.newMapBinder(binder(), String.class,
                ActionInterceptor.class);

        interceptors.addBinding(SearchAction.INSTANCE.name()).to(SearchInterceptor.class);
        interceptors.addBinding(IndexAction.INSTANCE.name()).to(IndexInterceptor.class);

        install(new DisruptorMessageBusModule());
        install(new ProcessorsModule());
        install(new ElasticConversionModule());

        bind(RyftRestClient.class).in(Singleton.class);
        bind(RyftPluginGlobalSettingsProvider.class).in(Singleton.class);

    }

}
