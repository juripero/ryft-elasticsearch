package com.ryft.elasticsearch.plugin.disruptor.messages;

import com.ryft.elasticsearch.converter.ryftdsl.RyftQuery;
import com.ryft.elasticsearch.plugin.RyftProperties;
import java.util.List;
import java.util.Map;
import org.elasticsearch.cluster.routing.ShardRouting;

/**
 *
 * @author denis
 */
public interface IndexSearchRequestEventFactory {

    public IndexSearchRequestEvent create(RyftProperties ryftProperties, 
            RyftQuery ryftQuery, List<ShardRouting> shards, Map<String, Object> parsedQuery);

}
