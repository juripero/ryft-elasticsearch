package com.ryft.elasticsearch.plugin.disruptor.messages;

import com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl.RyftQuery;
import java.util.List;
import org.elasticsearch.cluster.routing.ShardRouting;

/**
 *
 * @author denis
 */
public interface RyftClusterRequestEventFactory {

    public RyftClusterRequestEvent create(RyftQuery ryftQuery, List<ShardRouting> shards);

}
