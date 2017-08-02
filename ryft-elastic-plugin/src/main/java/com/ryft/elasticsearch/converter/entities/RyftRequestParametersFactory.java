package com.ryft.elasticsearch.converter.entities;

import com.ryft.elasticsearch.converter.ryftdsl.RyftQuery;
import java.util.List;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;

public interface RyftRequestParametersFactory {
    
    public RyftRequestParameters create(RyftQuery ryftQuery, String[] indices, List<AbstractAggregationBuilder> aggregationBuilders);

}
