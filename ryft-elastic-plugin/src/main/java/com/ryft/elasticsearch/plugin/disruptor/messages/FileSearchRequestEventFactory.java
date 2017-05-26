package com.ryft.elasticsearch.plugin.disruptor.messages;

import com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl.RyftQuery;
import com.ryft.elasticsearch.plugin.elastic.plugin.RyftProperties;

public interface FileSearchRequestEventFactory {

    public FileSearchRequestEvent create(RyftProperties ryftProperties, 
            RyftQuery ryftQuery);

}
