package com.ryft.elasticsearch.plugin.processors;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import com.ryft.elasticsearch.plugin.disruptor.messages.InternalEvent;
import com.ryft.elasticsearch.plugin.disruptor.messages.RyftClusterRequestEvent;
import com.ryft.elasticsearch.plugin.elastic.plugin.PropertiesProvider;
import com.ryft.elasticsearch.plugin.elastic.plugin.mappings.RyftResponse;
import com.ryft.elasticsearch.plugin.elastic.plugin.rest.client.ClusterRestClientHandler;
import com.ryft.elasticsearch.plugin.elastic.plugin.rest.client.NettyUtils;
import com.ryft.elasticsearch.plugin.elastic.plugin.rest.client.RyftRestClient;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;

@Singleton
public class RyftRequestProcessor extends RyftProcessor {

    private static final ESLogger LOGGER = Loggers.getLogger(RyftRequestProcessor.class);
    private final RyftRestClient channelProvider;
    private final PropertiesProvider props;

    @Inject
    public RyftRequestProcessor(PropertiesProvider properties, RyftRestClient channelProvider) {
        this.props = properties;
        this.channelProvider = channelProvider;
    }

    @Override
    public void process(InternalEvent event) {
        RyftClusterRequestEvent requestEvent = (RyftClusterRequestEvent) event;
        executor.submit(() -> executeRequest(requestEvent));
    }

    protected void executeRequest(RyftClusterRequestEvent requestEvent) {
        try {
            List<ShardRouting> shardRoutings = requestEvent.getShards();
            Map<Integer, List<ShardRouting>> groupedShards = shardRoutings.stream()
                    .collect(Collectors.groupingBy(ShardRouting::getId));
            Long start = System.currentTimeMillis();
            Map<ShardRouting, RyftResponse> ryftResponses = sendToRyftCluster(requestEvent, groupedShards);
            Long searchTime = System.currentTimeMillis() - start;
            SearchResponse searchResponse = getSearchResponse(requestEvent, groupedShards,
                    ryftResponses, searchTime);
            requestEvent.getCallback().onResponse(searchResponse);
        } catch (InterruptedException ex) {
            LOGGER.error("Failed to connect to ryft server", ex);
            requestEvent.getCallback().onFailure(ex);
        }
    }

    private Map<ShardRouting, RyftResponse> sendToRyftCluster(RyftClusterRequestEvent requestEvent,
            Map<Integer, List<ShardRouting>> groupedShards) throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(groupedShards.size());

        Map<Integer, Optional<ChannelFuture>> ryftChannelFutures = groupedShards.entrySet().stream().map(entry -> {
            Optional<ChannelFuture> maybeRyftChannelFuture = sendToRyft(requestEvent, entry.getValue(), countDownLatch);
            return new AbstractMap.SimpleEntry<Integer, Optional<ChannelFuture>>(entry.getKey(), maybeRyftChannelFuture);
        }).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));

        countDownLatch.await();

        return ryftChannelFutures.entrySet().stream().map(entry -> {
            RyftResponse ryftResponse = entry.getValue().map(channelFuture
                    -> NettyUtils.getAttribute(channelFuture.channel(), ClusterRestClientHandler.RYFT_RESPONSE_ATTR))
                    .orElse(new RyftResponse(null, null, null, String.format("Can not get results for shard %d", entry.getKey())));
            ShardRouting indexShard = entry.getValue().map(channelFuture
                    -> NettyUtils.getAttribute(channelFuture.channel(), ClusterRestClientHandler.INDEX_SHARD_ATTR)).get();
            return new AbstractMap.SimpleEntry<ShardRouting, RyftResponse>(indexShard, ryftResponse);
        }).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    }

    private Optional<ChannelFuture> sendToRyft(RyftClusterRequestEvent requestEvent,
            List<ShardRouting> shards, CountDownLatch countDownLatch) {
        ShardRouting shard = shards.stream().findFirst().get();
        shards.remove(shard);
        if (shard != null) {
            URI uri;
            try {
                uri = requestEvent.getRyftSearchURL(shard);
                Optional<ChannelFuture> maybeRyftResponse = sendToRyft(uri, shard, countDownLatch);
                if (maybeRyftResponse.isPresent()) {
                    return maybeRyftResponse;
                } else {
                    return sendToRyft(requestEvent, shards, countDownLatch);
                }
            } catch (URISyntaxException ex) {
                LOGGER.error("Can not get search URL", ex);
                return sendToRyft(requestEvent, shards, countDownLatch);
            }
        } else {
            countDownLatch.countDown();
            return Optional.empty();
        }
    }

    private Optional<ChannelFuture> sendToRyft(URI searchUri, ShardRouting shardRouting, CountDownLatch countDownLatch) {
        LOGGER.info("Search in shard: {}", shardRouting);
        return channelProvider.get(searchUri.getHost()).map((ryftChannel) -> {
            NettyUtils.setAttribute(ClusterRestClientHandler.INDEX_SHARD_ATTR, shardRouting, ryftChannel);
            ryftChannel.pipeline().addLast(new ClusterRestClientHandler(countDownLatch));
            DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, searchUri.toString());
            request.headers().add(HttpHeaders.Names.AUTHORIZATION, "Basic " + props.get().getStr(PropertiesProvider.RYFT_REST_AUTH));
            request.headers().add(HttpHeaders.Names.HOST, String.format("%s:%d", searchUri.getHost(), searchUri.getPort()));
            LOGGER.info("Send request: {}", request);
            ChannelFuture cf = ryftChannel.writeAndFlush(request);
            return cf;
        });
    }

    private SearchResponse getSearchResponse(RyftClusterRequestEvent requestEvent,
            Map<Integer, List<ShardRouting>> groupedShards,
            Map<ShardRouting, RyftResponse> ryftResponses, Long searchTime) throws InterruptedException {
        Map<ShardRouting, RyftResponse> errorResponses = ryftResponses.entrySet().stream()
                .filter(entry -> entry.getValue().hasErrors())
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        Map<ShardRouting, RyftResponse> resultResponses = ryftResponses.entrySet().stream()
                .filter(entry -> !entry.getValue().hasErrors())
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        if (!errorResponses.isEmpty()) {
            LOGGER.warn("Receive errors from shards: {}", errorResponses.toString());
            Map<Integer, List<ShardRouting>> shardsToSearch = new HashMap<>();
            errorResponses.entrySet().stream().forEach((entry) -> {
                Integer shardId = entry.getKey().getId();
                List<ShardRouting> shards = groupedShards.get(shardId);
                if (!shards.isEmpty()) {
                    shardsToSearch.put(shardId, shards);
                }
            });
            if (shardsToSearch.isEmpty()) {
                LOGGER.info("No more replicas to search");
                return constructSearchResponse(ryftResponses, searchTime);
            } else {
                LOGGER.info("Retry search requests to replica");
                Long start = System.currentTimeMillis();
                Map<ShardRouting, RyftResponse> result = sendToRyftCluster(requestEvent, shardsToSearch);
                searchTime += System.currentTimeMillis() - start;
                resultResponses.putAll(result);
                return getSearchResponse(requestEvent, shardsToSearch, resultResponses, searchTime);
            }
        } else {
            return constructSearchResponse(ryftResponses, searchTime);
        }
    }

    private SearchResponse constructSearchResponse(Map<ShardRouting, RyftResponse> resultResponses, Long tookInMillis) {
        List<InternalSearchHit> searchHits = new ArrayList<>();
        List<ShardSearchFailure> failures = new ArrayList<>();
        Integer totalShards = 0;
        Integer failureShards = 0;
        Long totalHits = 0l;
        for (Entry<ShardRouting, RyftResponse> entry : resultResponses.entrySet()) {
            totalShards += 1;
            RyftResponse ryftResponse = entry.getValue();
            ShardRouting shardRouting = entry.getKey();
            String errorMessage = ryftResponse.getMessage();
            String[] errors = ryftResponse.getErrors();
            SearchShardTarget searchShardTarget = getSearchShardTarget(shardRouting);
            if (ryftResponse.hasErrors()) {
                failureShards += 1;
                if ((errorMessage != null) && (!errorMessage.isEmpty())) {
                    failures.add(new ShardSearchFailure(new Exception(errorMessage), searchShardTarget));
                }
                if ((errors != null) && (errors.length > 0)) {
                    Stream.of(errors)
                            .map(error -> new ShardSearchFailure(new Exception(error), searchShardTarget))
                            .collect(Collectors.toCollection(() -> failures));
                }
            }
            if (ryftResponse.hasResults()) {
                ryftResponse.getResults().stream().map(hit -> processSearchResult(hit, searchShardTarget))
                        .collect(Collectors.toCollection(() -> searchHits));
            }
            if ((ryftResponse.getStats() != null) && (ryftResponse.getStats().getMatches() != null)) {
                totalHits += ryftResponse.getStats().getMatches();
            }
        }
        InternalSearchHits hits = new InternalSearchHits(
                searchHits.toArray(new InternalSearchHit[searchHits.size()]),
                totalHits == 0 ? searchHits.size() : totalHits, 1.0f);

        InternalSearchResponse internalSearchResponse = new InternalSearchResponse(hits, InternalAggregations.EMPTY,
                null, null, false, false);

        SearchResponse searchResponse = new SearchResponse(internalSearchResponse, null, totalShards, totalShards - failureShards, tookInMillis,
                failures.toArray(new ShardSearchFailure[failures.size()]));

        return searchResponse;
    }

    private SearchShardTarget getSearchShardTarget(ShardRouting shardRouting) {
        return new SearchShardTarget(shardRouting.currentNodeId(), shardRouting.index(), shardRouting.getId());
    }

    private InternalSearchHit processSearchResult(ObjectNode hit, SearchShardTarget searchShardTarget) {
        String uid = hit.has("_uid") ? hit.get("_uid").asText() : UUID.randomUUID().toString();
        String type = hit.has("type") ? hit.get("type").asText() : "";

        InternalSearchHit searchHit = new InternalSearchHit(0, uid, new Text(type),
                ImmutableMap.of());
        searchHit.shardTarget(searchShardTarget);

        String error = hit.has("error") ? hit.get("error").asText() : "";
        if (!error.isEmpty()) {
            searchHit.sourceRef(new BytesArray("{\"error\": \"" + error + "\"}"));
        } else {
            hit.remove("_index");
            searchHit.sourceRef(new BytesArray(hit.toString()));
        }
        return searchHit;
    }

    @Override
    public int getPoolSize() {
        return props.get().getInt(PropertiesProvider.REQ_THREAD_NUM);
    }

    @Override
    public String getName() {
        return String.format("ryft-request-pool-%d", getPoolSize());
    }

}
