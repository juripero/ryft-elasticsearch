
package com.ryft.elasticsearch.rest.processors;

import com.ryft.elasticsearch.plugin.disruptor.messages.RequestEvent;


public class RyftSearchContext<T extends RequestEvent>{
    
    private final T requestEvent;

    public RyftSearchContext(T requestEvent) {
        this.requestEvent = requestEvent;
    }
    
    

}
