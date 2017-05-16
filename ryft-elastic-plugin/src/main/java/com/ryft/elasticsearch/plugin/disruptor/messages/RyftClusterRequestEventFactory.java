package com.ryft.elasticsearch.plugin.disruptor.messages;

import com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl.RyftQuery;
import com.ryft.elasticsearch.plugin.elastic.plugin.RyftProperties;
import java.util.List;
import org.elasticsearch.cluster.routing.ShardRouting;

/**
 *
 * @author denis
 */
public interface RyftClusterRequestEventFactory {

    public RyftClusterRequestEvent create(RyftProperties ryftProperties, 
            RyftQuery ryftQuery, List<ShardRouting> shards);

}
