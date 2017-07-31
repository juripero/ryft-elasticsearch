package com.ryft.elasticsearch.plugin.disruptor.messages;

import com.ryft.elasticsearch.converter.ryftdsl.RyftQuery;
import com.ryft.elasticsearch.plugin.RyftProperties;
import java.util.List;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.search.aggregations.AggregationBuilder;

/**
 *
 * @author denis
 */
public interface IndexSearchRequestEventFactory {

    public IndexSearchRequestEvent create(RyftProperties ryftProperties, 
            RyftQuery ryftQuery, List<ShardRouting> shards, List<AggregationBuilder> aggregations);

}
