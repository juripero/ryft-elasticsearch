package com.ryft.elasticsearch.plugin.disruptor.messages;

import com.ryft.elasticsearch.converter.ryftdsl.RyftQuery;
import com.ryft.elasticsearch.plugin.RyftProperties;
import java.util.List;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;

public interface FileSearchRequestEventFactory {

    public FileSearchRequestEvent create(RyftProperties ryftProperties, 
            RyftQuery ryftQuery, List<AbstractAggregationBuilder> aggregationBuilders);

}
