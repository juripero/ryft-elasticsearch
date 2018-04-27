/**
 * ##License
 * Ryft-Customized BSD License
 * Copyright (c) 2018, Ryft Systems, Inc.
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation and/or
 *   other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software must display the following acknowledgement:
 *   This product includes software developed by Ryft Systems, Inc.
 * 4. Neither the name of Ryft Systems, Inc. nor the names of its contributors may be used
 *   to endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY RYFT SYSTEMS, INC. ''AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL RYFT SYSTEMS, INC. BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
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
