package com.ryft.elasticsearch.rest.processors;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

import com.ryft.elasticsearch.plugin.disruptor.messages.EventType;

public class ProcessorsModule extends AbstractModule {
    
    @Override
    protected void configure() {
        
        MapBinder<EventType, RyftProcessor> processors = MapBinder.newMapBinder(binder(), EventType.class, RyftProcessor.class);
        processors.addBinding(EventType.INDEX_SEARCH_REQUEST).to(RyftSearchRequestProcessor.class);
        processors.addBinding(EventType.FILE_SEARCH_REQUEST).to(RyftSearchRequestProcessor.class);
    }

}
