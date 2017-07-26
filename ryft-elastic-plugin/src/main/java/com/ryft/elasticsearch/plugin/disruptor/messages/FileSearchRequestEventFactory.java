package com.ryft.elasticsearch.plugin.disruptor.messages;

import com.ryft.elasticsearch.converter.entities.AggregationParameters;
import com.ryft.elasticsearch.converter.ryftdsl.RyftQuery;
import com.ryft.elasticsearch.plugin.RyftProperties;

public interface FileSearchRequestEventFactory {

    public FileSearchRequestEvent create(RyftProperties ryftProperties, 
            RyftQuery ryftQuery, AggregationParameters agg);

}
