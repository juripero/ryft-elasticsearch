package com.ryft.elasticsearch.rest.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ryft.elasticsearch.plugin.disruptor.messages.FileSearchRequestEvent;
import com.ryft.elasticsearch.plugin.service.AggregationService;
import io.netty.channel.ChannelFuture;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import com.ryft.elasticsearch.plugin.disruptor.messages.IndexSearchRequestEvent;
import com.ryft.elasticsearch.plugin.disruptor.messages.RequestEvent;
import com.ryft.elasticsearch.plugin.PropertiesProvider;
import com.ryft.elasticsearch.rest.mappings.RyftResponse;
import com.ryft.elasticsearch.rest.client.ClusterRestClientHandler;
import com.ryft.elasticsearch.rest.client.NettyUtils;
import com.ryft.elasticsearch.rest.client.RyftRestClient;
import com.ryft.elasticsearch.rest.client.RyftSearchException;

import java.net.URI;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.search.SearchShardTarget;

@Singleton
public class SearchRequestProcessor extends RyftProcessor {

    private static final ESLogger LOGGER = Loggers.getLogger(SearchRequestProcessor.class);

    @Inject
    public SearchRequestProcessor(PropertiesProvider properties, RyftRestClient channelProvider,
            AggregationService aggregationService) {
        super(properties, channelProvider, aggregationService);
    }

    @Override
    protected SearchResponse executeRequest(RequestEvent event) throws RyftSearchException {
        if (event instanceof IndexSearchRequestEvent) {
            try {
                return executeRequest((IndexSearchRequestEvent) event);
            } catch (InterruptedException | JsonProcessingException ex) {
                throw new RyftSearchException(ex);
            }
        }
        if (event instanceof FileSearchRequestEvent) {
            try {
                return executeRequest((FileSearchRequestEvent) event);
            } catch (InterruptedException ex) {
                throw new RyftSearchException(ex);
            }
        }
        throw new RyftSearchException("Unknown request event");
    }

    private SearchResponse executeRequest(IndexSearchRequestEvent requestEvent)
            throws InterruptedException, RyftSearchException, JsonProcessingException {
        Map<Integer, List<ShardRouting>> groupedShards = requestEvent.getShards().stream()
                .filter(shard -> shard.started())
                .collect(Collectors.groupingBy(ShardRouting::getId));
        return getSearchResponse(requestEvent, groupedShards);
    }

    private SearchResponse executeRequest(FileSearchRequestEvent requestEvent)
            throws InterruptedException, RyftSearchException {
        Long start = System.currentTimeMillis();
        Map<SearchShardTarget, RyftResponse> resultResponses = sendToRyft(requestEvent);
        Long searchTime = System.currentTimeMillis() - start;
        return constructSearchResponse(requestEvent, resultResponses, searchTime);
    }

    private SearchResponse getSearchResponse(IndexSearchRequestEvent requestEvent,
            Map<Integer, List<ShardRouting>> groupedShards) throws InterruptedException, RyftSearchException, JsonProcessingException {
        if (aggregationService.allAggregationsSupportedByRyft(requestEvent)) {
            LOGGER.info("Ryft Server selected as aggregation backend");
            ObjectMapper mapper = new ObjectMapper();
            String jsonString = mapper.writeValueAsString(aggregationService.getAggregationsFromEvent(requestEvent));
            requestEvent.setRyftSupportedAggregationQuery(jsonString);
        }

        Long start = System.currentTimeMillis();
        Map<SearchShardTarget, RyftResponse> ryftResponses = sendToRyft(requestEvent, groupedShards);
        Long searchTime = System.currentTimeMillis() - start;
        return getSearchResponse(requestEvent, groupedShards,
                ryftResponses, searchTime);
    }

    private SearchResponse getSearchResponse(IndexSearchRequestEvent requestEvent,
            Map<Integer, List<ShardRouting>> groupedShards,
            Map<SearchShardTarget, RyftResponse> ryftResponses, Long searchTime) throws InterruptedException, RyftSearchException {
        Map<SearchShardTarget, RyftResponse> errorResponses = ryftResponses.entrySet().stream()
                .filter(entry -> entry.getValue().hasErrors())
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        Map<SearchShardTarget, RyftResponse> resultResponses = ryftResponses.entrySet().stream()
                .filter(entry -> !entry.getValue().hasErrors())
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        if (!errorResponses.isEmpty()) {
            LOGGER.warn("Receive errors from shards: {}", errorResponses.toString());
            Map<Integer, List<ShardRouting>> shardsToSearch = new HashMap<>();
            errorResponses.forEach((key, value) -> {
                Integer shardId = key.shardId();
                List<ShardRouting> shards = groupedShards.get(shardId);
                if (!shards.isEmpty()) {
                    shardsToSearch.put(shardId, shards);
                }
            });
            if (shardsToSearch.isEmpty()) {
                LOGGER.info("No more replicas to search. Search time: {}", searchTime);
                return constructSearchResponse(requestEvent, ryftResponses, searchTime);
            } else {
                LOGGER.info("Retry search requests to error shards.");
                Long start = System.currentTimeMillis();
                Map<SearchShardTarget, RyftResponse> result = sendToRyft(requestEvent, shardsToSearch);
                searchTime += System.currentTimeMillis() - start;
                resultResponses.putAll(result);
                return getSearchResponse(requestEvent, shardsToSearch, resultResponses, searchTime);
            }
        } else {
            LOGGER.info("Search successful. Search time: {}", searchTime);
            return constructSearchResponse(requestEvent, ryftResponses, searchTime);
        }
    }

    private Map<SearchShardTarget, RyftResponse> sendToRyft(FileSearchRequestEvent requestEvent) throws InterruptedException, RyftSearchException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Optional<ChannelFuture> maybeChannelFuture = sendToRyft(requestEvent.getRyftSearchURL(), null, countDownLatch);
        countDownLatch.await();
        if (maybeChannelFuture.isPresent()) {
            ChannelFuture channelFuture = maybeChannelFuture.get();
            RyftResponse ryftResponse = NettyUtils.getAttribute(channelFuture.channel(), ClusterRestClientHandler.RYFT_RESPONSE_ATTR);
            String nodeName = ((ryftResponse.getStats() == null) || (ryftResponse.getStats().getHost() == null))
                    ? "RYFT-service" : ryftResponse.getStats().getHost();
            String indexName = requestEvent.getFilenames().stream().collect(Collectors.joining(","));
            SearchShardTarget searchShardTarget = new SearchShardTarget(nodeName, indexName, 0);
            Map<SearchShardTarget, RyftResponse> resultResponses = new HashMap();
            resultResponses.put(searchShardTarget, ryftResponse);
            return resultResponses;
        } else {
            throw new RyftSearchException("Can not get response from RYFT");
        }
    }

    private Map<SearchShardTarget, RyftResponse> sendToRyft(IndexSearchRequestEvent requestEvent,
            Map<Integer, List<ShardRouting>> groupedShards) throws InterruptedException {

        CountDownLatch countDownLatch;
        Map<Integer, Optional<ChannelFuture>> ryftChannelFutures;

        countDownLatch = new CountDownLatch(groupedShards.size());

        ryftChannelFutures = groupedShards.entrySet().stream().map(entry -> {
            Optional<ChannelFuture> maybeRyftChannelFuture = sendToRyft(requestEvent, entry.getValue(), countDownLatch);
            return new AbstractMap.SimpleEntry<>(entry.getKey(), maybeRyftChannelFuture);
        }).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));

        countDownLatch.await();

        return ryftChannelFutures.entrySet().stream().map(entry -> {
            RyftResponse ryftResponse = entry.getValue().map(channelFuture
                    -> NettyUtils.getAttribute(channelFuture.channel(), ClusterRestClientHandler.RYFT_RESPONSE_ATTR))
                    .orElse(new RyftResponse(null, null, null, String.format("Can not get results for shard %d", entry.getKey())));
            ShardRouting indexShard = entry.getValue().map(channelFuture
                    -> NettyUtils.getAttribute(channelFuture.channel(), ClusterRestClientHandler.INDEX_SHARD_ATTR)).get();
            SearchShardTarget searchShardTarget = getSearchShardTarget(indexShard);
            return new AbstractMap.SimpleEntry<>(searchShardTarget, ryftResponse);
        }).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    }

    private Optional<ChannelFuture> sendToRyft(IndexSearchRequestEvent requestEvent,
            List<ShardRouting> shards, CountDownLatch countDownLatch) {
        ShardRouting shard = shards.stream().findAny().get();
        shards.remove(shard);
        if (shard != null) {
            URI uri;
            try {
                uri = requestEvent.getRyftSearchURL(shard);
                Optional<ChannelFuture> maybeRyftResponse = sendToRyft(uri, shard, countDownLatch);
                if (maybeRyftResponse.isPresent()) {
                    return maybeRyftResponse;
                } else {
                    LOGGER.info("Attempt to search on other shard.");
                    return sendToRyft(requestEvent, shards, countDownLatch);
                }
            } catch (RyftSearchException ex) {
                LOGGER.error("Can not get search URL", ex);
                return sendToRyft(requestEvent, shards, countDownLatch);
            }
        } else {
            countDownLatch.countDown();
            return Optional.empty();
        }
    }

    private SearchShardTarget getSearchShardTarget(ShardRouting shardRouting) {
        return new SearchShardTarget(shardRouting.currentNodeId(), shardRouting.index(), shardRouting.getId());
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
