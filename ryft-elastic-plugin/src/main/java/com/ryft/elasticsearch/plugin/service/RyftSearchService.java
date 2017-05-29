package com.ryft.elasticsearch.plugin.service;

import com.ryft.elasticsearch.plugin.disruptor.messages.FileSearchRequestEventFactory;
import com.ryft.elasticsearch.converter.entities.RyftRequestParameters;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.common.inject.Inject;
import com.ryft.elasticsearch.plugin.disruptor.messages.IndexSearchRequestEventFactory;
import com.ryft.elasticsearch.plugin.disruptor.messages.RequestEvent;

public class RyftSearchService {

    private final IndexSearchRequestEventFactory indexSearchRequestEventFactory;
    private final FileSearchRequestEventFactory fileSearchRequestEventFactory;
    private final ClusterService clusterService;

    @Inject
    public RyftSearchService(ClusterService clusterService,
            IndexSearchRequestEventFactory indexSearchRequestEventFactory,
            FileSearchRequestEventFactory fileSearchRequestEventFactory) {
        this.clusterService = clusterService;
        this.indexSearchRequestEventFactory = indexSearchRequestEventFactory;
        this.fileSearchRequestEventFactory = fileSearchRequestEventFactory;
    }

    public RequestEvent getClusterRequestEvent(RyftRequestParameters requestParameters) {
        if (requestParameters.isFileSearch()) {
            return fileSearchRequestEventFactory.create(requestParameters.getRyftProperties(),
                    requestParameters.getQuery());
        } else {
            return indexSearchRequestEventFactory.create(requestParameters.getRyftProperties(),
                    requestParameters.getQuery(), getShards(requestParameters));
        }
    }

    private List<ShardRouting> getShards(RyftRequestParameters requestParameters) {
        ShardsIterator shardsIterator = clusterService.state().getRoutingTable().allShards(requestParameters.getIndices());
        List<ShardRouting> shardRoutingList = StreamSupport.stream(shardsIterator.asUnordered().spliterator(), false)
                .collect(Collectors.toList());
        return shardRoutingList;
    }
}
