package com.ryft.elasticsearch.plugin.disruptor.messages;

import com.ryft.elasticsearch.converter.entities.RyftRequestParameters;
import java.util.List;
import org.elasticsearch.cluster.routing.ShardRouting;

/**
 *
 * @author denis
 */
public interface IndexSearchRequestEventFactory {

    public IndexSearchRequestEvent create(RyftRequestParameters requestParameters, List<ShardRouting> shards);

}
