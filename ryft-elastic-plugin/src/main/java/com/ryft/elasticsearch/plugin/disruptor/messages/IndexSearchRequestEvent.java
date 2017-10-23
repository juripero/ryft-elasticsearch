package com.ryft.elasticsearch.plugin.disruptor.messages;

import com.ryft.elasticsearch.converter.entities.RyftRequestParameters;
import com.ryft.elasticsearch.plugin.ObjectMapperFactory;
import com.ryft.elasticsearch.rest.client.RyftSearchException;
import com.ryft.elasticsearch.rest.mappings.RyftRequestPayload;
import com.ryft.elasticsearch.rest.mappings.RyftRequestPayload.Tweaks.ClusterRoute;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchShardTarget;

public class IndexSearchRequestEvent extends SearchRequestEvent {

    private static final ESLogger LOGGER = Loggers.getLogger(IndexSearchRequestEvent.class);

    private final Settings settings;

    private final List<ShardRouting> shards;

    private final List<String> failedNodes = new ArrayList<>();

    @Override
    public EventType getEventType() {
        return EventType.INDEX_SEARCH_REQUEST;
    }

    @Inject
    public IndexSearchRequestEvent(ClusterService clusterService,
            Settings settings, ObjectMapperFactory objectMapperFactory,
            @Assisted RyftRequestParameters requestParameters) throws RyftSearchException {
        super(clusterService, objectMapperFactory, requestParameters);
        this.settings = settings;
        ShardsIterator shardsIterator = clusterState.getRoutingTable().allShards(requestParameters.getIndices());
        shards = StreamSupport.stream(shardsIterator.asUnordered().spliterator(), false)
                .collect(Collectors.toList());
    }

    @Override
    protected RyftRequestPayload getRyftRequestPayload() throws RyftSearchException {
        Collection<SearchShardTarget> shards = getShardsToSearch();
        RyftRequestPayload payload = new RyftRequestPayload();
        payload.setTweaks(getTweaks(shards));
        if (canBeAggregatedByRYFT()) {
            LOGGER.info("Ryft Server selected as aggregation backend");
            payload.setAggs(getAggregations());
        }
        return payload;
    }

    @Override
    protected URI getRyftSearchURL() throws RyftSearchException {
        validateRequest();
        try {
            return new URI("http://"
                    + clusterState.getNodes().getLocalNode().getHostAddress() + ":" + getPort()
                    + "/search?query=" + getEncodedQuery()
                    + "&local=false&file&stats=true"
                    + "&cs=" + getCaseSensitive()
                    + "&format=" + getFormat().name().toLowerCase()
                    + "&limit=" + getLimit());
        } catch (URISyntaxException ex) {
            throw new RyftSearchException("Ryft search URL composition exceptoion", ex);
        }
    }

    private RyftRequestPayload.Tweaks getTweaks(Collection<SearchShardTarget> shards) {
        RyftRequestPayload.Tweaks tweaks = new RyftRequestPayload.Tweaks();
        tweaks.setClusterRoutes(shards.stream()
                .collect(Collectors.groupingBy(sarchTarget -> sarchTarget.nodeId()))
                .entrySet().stream().map(e -> getClusterRoute(e.getKey(), e.getValue()))
                .collect(Collectors.toList()));
        return tweaks;
    }

    private ClusterRoute getClusterRoute(String nodeId, Collection<SearchShardTarget> shards) {
        ClusterRoute result = new ClusterRoute();
        result.setLocation(
                "http://" + clusterState.getNodes().get(nodeId).getHostName() + ":" + getPort());
        result.setFiles(getFilenames(shards));
        return result;
    }

    ///{clustername}/nodes/{nodenumber}/indices/{indexname}/{shardid}/index/*.{indexname}jsonfld
    public List<String> getFilenames(Collection<SearchShardTarget> shards) {
        String dataPath = settings.get("path.data");
        String pathBegin;
        if ((dataPath != null) && (!dataPath.isEmpty())) {
            pathBegin = dataPath.replaceFirst("^\\/ryftone\\/", "");
        } else {
            pathBegin = "";
        }
        return shards.stream().map(shardTarget
                -> String.format("%1$s/%2$s/nodes/0/indices/%3$s/%4$d/index/*.%3$sjsonfld",
                        pathBegin,
                        clusterState.getClusterName().value(),
                        shardTarget.getIndex(),
                        shardTarget.getShardId())
        ).collect(Collectors.toList());
    }

    public List<ShardRouting> getAllShards() {
        return shards;
    }

    public List<SearchShardTarget> getShardsToSearch() {
        return shards.stream()
                .filter(shard -> !failedNodes.contains(shard.currentNodeId()))
                .collect(Collectors.groupingBy(shard -> shard.id())).values().stream()
                .map(shardList -> {
                    ShardRouting shard = shardList.get(0);
                    return new SearchShardTarget(shard.currentNodeId(), shard.getIndex(), shard.id());
                }).collect(Collectors.toList());
    }

    public void addFailedNode(String nodeId) {
        failedNodes.add(nodeId);
    }

    @Override
    public String toString() {
        return "IndexSearchRequestEvent{query=" + requestParameters.getQuery()
                + ", indices=" + Arrays.toString(requestParameters.getIndices())
                + ", shards=" + shards.size() + '}';
    }

}
