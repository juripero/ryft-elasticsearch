package com.dataart.ryft.processors;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

import com.dataart.ryft.disruptor.messages.EventType;

public class ProcessorsModule extends AbstractModule {
    
    @Override
    protected void configure() {
        
        MapBinder<EventType, RyftProcessor> processors = MapBinder.newMapBinder(binder(), EventType.class, RyftProcessor.class);
        processors.addBinding(EventType.ES_REQUEST).to(RyftRequestProcessor.class);
        
    }

}
