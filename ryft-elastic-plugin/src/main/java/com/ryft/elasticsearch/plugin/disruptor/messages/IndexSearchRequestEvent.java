/**
 * ##License
 * Ryft-Customized BSD License
 * Copyright (c) 2018, Ryft Systems, Inc.
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation and/or
 *   other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software must display the following acknowledgement:
 *   This product includes software developed by Ryft Systems, Inc.
 * 4. Neither the name of Ryft Systems, Inc. nor the names of its contributors may be used
 *   to endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY RYFT SYSTEMS, INC. ''AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL RYFT SYSTEMS, INC. BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.ryft.elasticsearch.plugin.disruptor.messages;

import com.ryft.elasticsearch.converter.entities.RyftRequestParameters;
import com.ryft.elasticsearch.plugin.ObjectMapperFactory;
import com.ryft.elasticsearch.rest.client.RyftSearchException;
import com.ryft.elasticsearch.rest.mappings.RyftRequestPayload;
import com.ryft.elasticsearch.rest.mappings.RyftRequestPayload.Tweaks.ClusterRoute;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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

    @Override
    public EventType getEventType() {
        return EventType.INDEX_SEARCH_REQUEST;
    }

    @Inject
    public IndexSearchRequestEvent(ClusterService clusterService,
            Settings settings, ObjectMapperFactory objectMapperFactory,
            @Assisted RyftRequestParameters requestParameters) {
        super(clusterService, objectMapperFactory, requestParameters);
        this.settings = settings;
    }

    @Override
    public RyftRequestPayload getRyftRequestPayload() {
        Collection<SearchShardTarget> shardsToSearch = getShardsToSearch();
        RyftRequestPayload payload = new RyftRequestPayload();
        payload.setTweaks(getTweaks(shardsToSearch));
        if (canBeAggregatedByRyft()) {
            LOGGER.info("Ryft Server selected as aggregation backend");
            payload.setAggs(getAggregations());
        }
        return payload;
    }

    @Override
    public URI getRyftSearchURL() throws RyftSearchException {
        validateRequest();
        try {
            if (!nodesToSearch.isEmpty()) {
                return new URI("http://"
                        + getHost() + ":" + getPort()
                        + "/search?query=" + getEncodedQuery()
                        + "&local=false&stats=true&ignore-missing-files=true"
                        + "&cs=" + getCaseSensitive()
                        + "&format=" + getFormat().name().toLowerCase()
                        + "&stream=true&limit=" + getSize());
            } else {
                throw new RyftSearchException("No RYFT nodes to search left");
            }
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
                "http://" + clusterService.state().getNodes().get(nodeId).getHostName() + ":" + getPort());
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
                        clusterService.state().getClusterName().value(),
                        shardTarget.getIndex(),
                        shardTarget.getShardId())
        ).collect(Collectors.toList());
    }

    public List<ShardRouting> getAllShards() {
        ShardsIterator shardsIterator = clusterService.state().getRoutingTable().allShards(requestParameters.getIndices());
        return StreamSupport.stream(shardsIterator.asUnordered().spliterator(), false)
                .filter(shard -> shard.started())
                .collect(Collectors.toList());
    }

    public List<SearchShardTarget> getShardsToSearch() {
        List<ShardRouting> filteredShards = getAllShards().stream()
                .filter(shard -> nodesToSearch.contains(clusterService.state().nodes().get(shard.currentNodeId()).getHostName()))
                .collect(Collectors.toList());
        Map<String, List<ShardRouting>> groupedShards = filteredShards.stream()
                .collect(Collectors.groupingBy(shard
                        -> String.format("%s_%d", shard.getIndex(), shard.getId())));
        List<SearchShardTarget> result = groupedShards.values().stream()
                .map(shardList -> shardList.get(0))
                .map(shard -> new SearchShardTarget(shard.currentNodeId(), shard.getIndex(), shard.id()))
                .collect(Collectors.toList());
        return result;
    }

    @Override
    public String toString() {
        return "IndexSearchRequestEvent{query=" + requestParameters.getQuery()
                + ", indices=" + Arrays.toString(requestParameters.getIndices())
                + ", shards=" + getAllShards().size() + '}';
    }

    public Long getShardsNumber() {
        Long result = getAllShards().stream()
                .filter(shard -> shard.primary())
                .collect(Collectors.counting());
        return result;
    }

    public boolean canBeExecuted() {
        return getShardsToSearch().size() == getShardsNumber();
    }

}
