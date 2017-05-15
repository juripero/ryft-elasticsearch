package com.ryft.elasticsearch.plugin.elastic.plugin.cluster;

import com.ryft.elasticsearch.plugin.disruptor.messages.RyftClusterRequestEvent;
import com.ryft.elasticsearch.plugin.disruptor.messages.RyftClusterRequestEventFactory;
import com.ryft.elasticsearch.plugin.elastic.converter.entities.RyftRequestParameters;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class RyftClusterService {
    
    private static final ESLogger LOGGER = Loggers.getLogger(RyftClusterService.class);

    private final RyftClusterRequestEventFactory ryftClusterRequestEventFactory;
    private final ClusterService clusterService;

    @Inject
    public RyftClusterService(ClusterService clusterService,
            RyftClusterRequestEventFactory ryftClusterRequestEventFactory) {
        this.clusterService = clusterService;
        this.ryftClusterRequestEventFactory = ryftClusterRequestEventFactory;
    }

    public RyftClusterRequestEvent getClusterRequestEvent(RyftRequestParameters requestParameters) {
        LOGGER.debug(clusterService.state().prettyPrint());
        RyftClusterRequestEvent event = ryftClusterRequestEventFactory.create(requestParameters.getQuery(), getShards(requestParameters));
        return event;
    }

    private List<ShardRouting> getShards(RyftRequestParameters requestParameters) {
        ShardsIterator shardsIterator = clusterService.state().getRoutingTable().allShards(requestParameters.getIndices());
        List<ShardRouting> shardRoutingList = StreamSupport.stream(shardsIterator.asUnordered().spliterator(), false)
                .collect(Collectors.toList());
        return shardRoutingList;
    }
}
