package com.ryft.elasticsearch.rest.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.ryft.elasticsearch.plugin.disruptor.messages.FileSearchRequestEvent;
import com.ryft.elasticsearch.plugin.disruptor.messages.SearchRequestEvent;
import com.ryft.elasticsearch.plugin.service.AggregationService;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

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
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;

@Singleton
public class SearchRequestProcessor extends RyftProcessor {

    private static final ESLogger LOGGER = Loggers.getLogger(SearchRequestProcessor.class);
    private final RyftRestClient channelProvider;
    private final PropertiesProvider props;
    private final AggregationService aggregationService;

    @Inject
    public SearchRequestProcessor(PropertiesProvider properties, RyftRestClient channelProvider,
                                  AggregationService aggregationService) {
        this.props = properties;
        this.channelProvider = channelProvider;
        this.aggregationService = aggregationService;
    }

    @Override
    public void process(RequestEvent event) {
        LOGGER.info("Processing event: {}", event);
        executor.submit(() -> {
            try {
                event.getCallback().onResponse(executeRequest(event));
            } catch (RyftSearchException | InterruptedException | RuntimeException | JsonProcessingException ex) {
                LOGGER.error("Request processing error", ex);
                event.getCallback().onFailure(ex);
            }
        });
    }

    private SearchResponse executeRequest(RequestEvent event)
            throws RyftSearchException, InterruptedException, JsonProcessingException {
        if (event instanceof IndexSearchRequestEvent) {
            return executeRequest((IndexSearchRequestEvent) event);
        }
        if (event instanceof FileSearchRequestEvent) {
            return executeRequest((FileSearchRequestEvent) event);
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
            requestEvent.setAggregationQuery(jsonString);
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

    private Map<SearchShardTarget, RyftResponse> sendToRyft(
            FileSearchRequestEvent requestEvent) throws InterruptedException, RyftSearchException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Optional<ChannelFuture> maybeChannelFuture = sendToRyft(requestEvent.getRyftSearchURL(), requestEvent.getAggregationQuery(), null, countDownLatch);
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

        if (requestEvent.getAggregationQuery() == null) {
            countDownLatch = new CountDownLatch(groupedShards.size());

            ryftChannelFutures = groupedShards.entrySet().stream().map(entry -> {
                Optional<ChannelFuture> maybeRyftChannelFuture = sendToRyft(requestEvent, entry.getValue(), countDownLatch);
                return new AbstractMap.SimpleEntry<>(entry.getKey(), maybeRyftChannelFuture);
            }).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        } else {
            //if we send aggregation request to Ryft server, we must also allow it to handle clustering. We will send request only to 1 shard
            countDownLatch = new CountDownLatch(1);
            ShardRouting chosenShard = groupedShards.entrySet().stream().findFirst().get().getValue().get(0);
            Optional<ChannelFuture> maybeRyftChannelFuture = sendToRyft(requestEvent, new ArrayList<>(Arrays.asList(chosenShard)), countDownLatch);
            ryftChannelFutures = Collections.singletonMap(0, maybeRyftChannelFuture);
        }

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
                Optional<ChannelFuture> maybeRyftResponse = sendToRyft(uri, requestEvent.getAggregationQuery(), shard, countDownLatch);
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

    private Optional<ChannelFuture> sendToRyft(URI searchUri, String aggregations, ShardRouting shardRouting, CountDownLatch countDownLatch) {
        LOGGER.info("Search in shard: {}", shardRouting);
        return channelProvider.get(searchUri.getHost()).map((ryftChannel) -> {
            NettyUtils.setAttribute(ClusterRestClientHandler.INDEX_SHARD_ATTR, shardRouting, ryftChannel);
            ryftChannel.pipeline().addLast(new ClusterRestClientHandler(countDownLatch));
            DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, searchUri.toString());

            if (aggregations != null) {
                String aggregationsBody = "{\"aggs\":" + aggregations + "}";
                ByteBuf bbuf = Unpooled.copiedBuffer(aggregationsBody, StandardCharsets.UTF_8);
                request.headers().set(HttpHeaders.Names.CONTENT_LENGTH, bbuf.readableBytes());
                request.content().clear().writeBytes(bbuf);
            }

            if (props.get().getBool(PropertiesProvider.RYFT_REST_AUTH_ENABLED)) {
                String login = props.get().getStr(PropertiesProvider.RYFT_REST_LOGIN);
                String password = props.get().getStr(PropertiesProvider.RYFT_REST_PASSWORD);
                String basicAuthToken = Base64.getEncoder().encodeToString(String.format("%s:%s", login, password).getBytes());
                request.headers().add(HttpHeaders.Names.AUTHORIZATION, "Basic " + basicAuthToken);
            }
            request.headers().add(HttpHeaders.Names.HOST, String.format("%s:%d", searchUri.getHost(), searchUri.getPort()));
            LOGGER.debug("Send request: {}", request);
            return ryftChannel.writeAndFlush(request);
        });
    }

    private SearchResponse constructSearchResponse(SearchRequestEvent requestEvent, Map<SearchShardTarget, RyftResponse> resultResponses, Long searchTime) throws RyftSearchException {
        List<InternalSearchHit> searchHitList = new ArrayList<>();
        List<ShardSearchFailure> failures = new ArrayList<>();
        Integer totalShards = 0;
        Integer failureShards = 0;
        ObjectNode ryftAggregationResults = null;
        for (Entry<SearchShardTarget, RyftResponse> entry : resultResponses.entrySet()) {
            totalShards += 1;
            RyftResponse ryftResponse = entry.getValue();
            SearchShardTarget searchShardTarget = entry.getKey();
            String errorMessage = ryftResponse.getMessage();
            String[] errors = ryftResponse.getErrors();

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
                ryftAggregationResults = ryftResponse.getStats().getExtra().getAggregations();
                ryftResponse.getResults().stream().map(
                        result -> processSearchResult(result, searchShardTarget)
                ).collect(Collectors.toCollection(() -> searchHitList));
            }
        }
        LOGGER.info("Search time: {} ms. Results: {}. Failures: {}", searchTime, searchHitList.size(), failures.size());
        InternalAggregations aggregations;
        if (requestEvent.getAggregationQuery() == null) {
            aggregations = aggregationService.applyAggregation(searchHitList, requestEvent);
        } else {
            aggregations = aggregationService.getFromRyftAggregations(requestEvent, ryftAggregationResults);
        }

        InternalSearchHit[] hits;
        if (requestEvent.getSize() != null) {
            hits = searchHitList.stream().limit(requestEvent.getSize()).toArray(InternalSearchHit[]::new);
        } else {
            hits = searchHitList.toArray(new InternalSearchHit[searchHitList.size()]);
        }
        InternalSearchHits internalSearchHits = new InternalSearchHits(hits, searchHitList.size(), Float.NEGATIVE_INFINITY);
        InternalSearchResponse internalSearchResponse = new InternalSearchResponse(internalSearchHits, aggregations,
                null, null, false, false);

        return new SearchResponse(internalSearchResponse, null, totalShards, totalShards - failureShards, searchTime,
                failures.toArray(new ShardSearchFailure[failures.size()]));
    }

    private SearchShardTarget getSearchShardTarget(ShardRouting shardRouting) {
        return new SearchShardTarget(shardRouting.currentNodeId(), shardRouting.index(), shardRouting.getId());
    }

    private InternalSearchHit processSearchResult(ObjectNode hit, SearchShardTarget searchShardTarget) {
        LOGGER.debug("Processing search result: {}", hit);
        InternalSearchHit searchHit;
        try {
            String uid = hit.has("_uid") ? hit.get("_uid").asText() : String.valueOf(hit.hashCode());
            String type = hit.has("type") ? hit.get("type").asText() : FileSearchRequestEvent.NON_INDEXED_TYPE;

            searchHit = new InternalSearchHit(0, uid, new Text(type),
                    ImmutableMap.of());
            searchHit.shardTarget(searchShardTarget);

            String error = hit.has("error") ? hit.get("error").asText() : "";
            if (!error.isEmpty()) {
                searchHit.sourceRef(new BytesArray("{\"error\": \"" + error + "\"}"));
            } else {
                hit.remove("_index");
                hit.remove("_uid");
                hit.remove("type");
                searchHit.sourceRef(new BytesArray(hit.toString()));
            }
            return searchHit;
        } catch (Exception ex) {
            LOGGER.error("Search result processing error.", ex);
            searchHit = new InternalSearchHit(0, "", new Text(""), ImmutableMap.of());
            searchHit.sourceRef(new BytesArray("{\"error\": \"" + ex.toString() + "\"}"));
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
