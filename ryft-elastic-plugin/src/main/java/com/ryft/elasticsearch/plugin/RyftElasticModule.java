package com.ryft.elasticsearch.plugin;

import com.ryft.elasticsearch.utils.JSR250Module;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.inject.multibindings.MapBinder;

import com.ryft.elasticsearch.plugin.disruptor.DisruptorMessageBusModule;
import com.ryft.elasticsearch.converter.ElasticConversionModule;
import com.ryft.elasticsearch.plugin.interceptors.ActionInterceptor;
import com.ryft.elasticsearch.plugin.interceptors.IndexInterceptor;
import com.ryft.elasticsearch.plugin.interceptors.SearchInterceptor;
import com.ryft.elasticsearch.plugin.service.AggregationService;
import com.ryft.elasticsearch.plugin.service.RyftSearchService;
import com.ryft.elasticsearch.rest.client.RyftRestClient;
import com.ryft.elasticsearch.rest.processors.ProcessorsModule;
import org.elasticsearch.client.transport.TransportClient;

/**
 *
 * @author imasternoy
 */
public class RyftElasticModule extends AbstractModule {

    @Override
    protected void configure() {
        install(new JSR250Module());

        bind(RyftProperties.class).toProvider(PropertiesProvider.class).in(Singleton.class);
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

        bind(TransportClient.class).toProvider(ElasticClientProvider.class).in(Singleton.class);
        bind(AggregationService.class);
        bind(RyftSearchService.class);
        bind(ObjectMapperFactory.class).in(Singleton.class);
    }
}
