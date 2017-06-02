package com.ryft.elasticsearch.rest.processors;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.ryft.elasticsearch.plugin.disruptor.messages.FileSearchRequestEvent;
import com.ryft.elasticsearch.plugin.disruptor.messages.SearchRequestEvent;
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
import com.ryft.elasticsearch.converter.ElasticConversionCriticalException;
import com.ryft.elasticsearch.plugin.PropertiesProvider;
import com.ryft.elasticsearch.rest.mappings.RyftResponse;
import com.ryft.elasticsearch.rest.client.ClusterRestClientHandler;
import com.ryft.elasticsearch.rest.client.NettyUtils;
import com.ryft.elasticsearch.rest.client.RyftRestClient;
import java.net.URI;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.histogram.*;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;

@Singleton
public class SearchRequestProcessor extends RyftProcessor {

    private static final ESLogger LOGGER = Loggers.getLogger(SearchRequestProcessor.class);
    private final RyftRestClient channelProvider;
    private final PropertiesProvider props;

    @Inject
    public SearchRequestProcessor(PropertiesProvider properties, RyftRestClient channelProvider) {
        this.props = properties;
        this.channelProvider = channelProvider;
    }

    @Override
    public void process(RequestEvent event) {
        executor.submit(() -> {
            try {
                event.getCallback().onResponse(executeRequest(event));
            } catch (InterruptedException | ElasticConversionCriticalException ex) {
                LOGGER.error("Request processing error", ex);
                event.getCallback().onFailure(ex);
            }
        });
    }

    private SearchResponse executeRequest(RequestEvent event)
            throws ElasticConversionCriticalException, InterruptedException {
        LOGGER.info("Got AGG!!");
        if (event instanceof IndexSearchRequestEvent) {
            return executeRequest((IndexSearchRequestEvent) event);
        }
        if (event instanceof FileSearchRequestEvent) {
            return executeRequest((FileSearchRequestEvent) event);
        }
        throw new ElasticConversionCriticalException("Unknown request event");
    }

    private SearchResponse executeRequest(IndexSearchRequestEvent requestEvent)
            throws InterruptedException, ElasticConversionCriticalException {
        Long start = System.currentTimeMillis();
        List<ShardRouting> shardRoutings = requestEvent.getShards();
        Map<Integer, List<ShardRouting>> groupedShards = shardRoutings.stream()
                .collect(Collectors.groupingBy(ShardRouting::getId));
        Map<SearchShardTarget, RyftResponse> ryftResponses = sendToRyft(requestEvent, groupedShards);
        Long searchTime = System.currentTimeMillis() - start;
        return getSearchResponse(requestEvent, groupedShards,
                ryftResponses, searchTime);
    }

    private SearchResponse executeRequest(FileSearchRequestEvent requestEvent)
            throws InterruptedException, ElasticConversionCriticalException {
        Long start = System.currentTimeMillis();
        Map<SearchShardTarget, RyftResponse> resultResponses = sendToRyft(requestEvent);
        Long searchTime = System.currentTimeMillis() - start;
        return constructSearchResponse(requestEvent, resultResponses, searchTime);
    }

    private Map<SearchShardTarget, RyftResponse> sendToRyft(
            FileSearchRequestEvent requestEvent) throws InterruptedException, ElasticConversionCriticalException {
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
            throw new ElasticConversionCriticalException("Can not get response from RYFT");
        }
    }

    private Map<SearchShardTarget, RyftResponse> sendToRyft(IndexSearchRequestEvent requestEvent,
            Map<Integer, List<ShardRouting>> groupedShards) throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(groupedShards.size());

        Map<Integer, Optional<ChannelFuture>> ryftChannelFutures = groupedShards.entrySet().stream().map(entry -> {
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
            } catch (ElasticConversionCriticalException ex) {
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
            if (props.get().getBool(PropertiesProvider.RYFT_REST_AUTH_ENABLED)) {
                String login = props.get().getStr(PropertiesProvider.RYFT_REST_LOGIN);
                String password = props.get().getStr(PropertiesProvider.RYFT_REST_PASSWORD);
                String basicAuthToken = Base64.getEncoder().encodeToString(String.format("%s:%s", login, password).getBytes());
                request.headers().add(HttpHeaders.Names.AUTHORIZATION, "Basic " + basicAuthToken);
            }
            request.headers().add(HttpHeaders.Names.HOST, String.format("%s:%d", searchUri.getHost(), searchUri.getPort()));
            LOGGER.info("Send request: {}", request);
            return ryftChannel.writeAndFlush(request);
        });
    }

    private SearchResponse getSearchResponse(IndexSearchRequestEvent requestEvent,
            Map<Integer, List<ShardRouting>> groupedShards,
            Map<SearchShardTarget, RyftResponse> ryftResponses, Long searchTime) throws InterruptedException {
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
                LOGGER.info("No more replicas to search");
                return constructSearchResponse(requestEvent, ryftResponses, searchTime);
            } else {
                LOGGER.info("Retry search requests to replica");
                Long start = System.currentTimeMillis();
                Map<SearchShardTarget, RyftResponse> result = sendToRyft(requestEvent, shardsToSearch);
                searchTime += System.currentTimeMillis() - start;
                resultResponses.putAll(result);
                return getSearchResponse(requestEvent, shardsToSearch, resultResponses, searchTime);
            }
        } else {
            return constructSearchResponse(requestEvent, ryftResponses, searchTime);
        }
    }

    private SearchResponse constructSearchResponse(SearchRequestEvent requestEvent, Map<SearchShardTarget, RyftResponse> resultResponses, Long tookInMillis) {
        List<InternalSearchHit> searchHits = new ArrayList<>();
        List<ShardSearchFailure> failures = new ArrayList<>();
        Integer totalShards = 0;
        Integer failureShards = 0;
        Long totalHits = 0L;
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
                IntStream.range(0, ryftResponse.getResults().size()).mapToObj(index
                        -> processSearchResult(ryftResponse.getResults().get(index), searchShardTarget, index)
                ).collect(Collectors.toCollection(() -> searchHits));
            }
            if ((ryftResponse.getStats() != null) && (ryftResponse.getStats().getMatches() != null)) {
                totalHits += ryftResponse.getStats().getMatches();
            }
        }
        InternalSearchHits hits = new InternalSearchHits(
                searchHits.toArray(new InternalSearchHit[searchHits.size()]),
                totalHits == 0 ? searchHits.size() : totalHits, Float.NEGATIVE_INFINITY);

        if (requestEvent.getAgg().isEmpty()) {
            InternalSearchResponse internalSearchResponse = new InternalSearchResponse(hits, InternalAggregations.EMPTY,
                    null, null, false, false);

            return new SearchResponse(internalSearchResponse, null, totalShards, totalShards - failureShards, tookInMillis,
                    failures.toArray(new ShardSearchFailure[failures.size()]));
        } else {
            List<InternalAggregation> aggregations = new ArrayList<>();
            InternalHistogram.Bucket bucket1 = new RyftHistogramFactory().createBucket(1388527200000L, 6, InternalAggregations.EMPTY, false, new ValueFormatter.DateTime("yyyy-MM-dd HH:mm:ss"));
            InternalHistogram.Bucket bucket2 = new RyftHistogramFactory().createBucket(1475269200000L, 1, InternalAggregations.EMPTY, false, new ValueFormatter.DateTime("yyyy-MM-dd HH:mm:ss"));

            List<Histogram.Bucket> buckets = new ArrayList<>();
            buckets.add(bucket1);
            buckets.add(bucket2);

            InternalHistogram histogram = new RyftHistogramFactory().create("2", buckets, null, 0L,
                    null,
                    ValueFormatter.RAW,
                    false,
                    Collections.emptyList(),
                    null);

            aggregations.add(histogram);
            InternalSearchResponse internalSearchResponse = new InternalSearchResponse(hits, new InternalAggregations(aggregations),
                    null, null, false, false);

            return new SearchResponse(internalSearchResponse, null, totalShards, totalShards - failureShards, tookInMillis,
                    failures.toArray(new ShardSearchFailure[failures.size()]));
        }

    }

    private SearchShardTarget getSearchShardTarget(ShardRouting shardRouting) {
        return new SearchShardTarget(shardRouting.currentNodeId(), shardRouting.index(), shardRouting.getId());
    }

    private InternalSearchHit processSearchResult(ObjectNode hit, SearchShardTarget searchShardTarget, Integer defaultId) {
        String uid = hit.has("_uid") ? hit.get("_uid").asText() : String.valueOf(defaultId);
        String type = hit.has("type") ? hit.get("type").asText() : "";

        InternalSearchHit searchHit = new InternalSearchHit(0, uid, new Text(type),
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
    }

    public static class RyftHistogramFactory extends InternalHistogram.Factory {

        public RyftHistogramFactory() {
            super();
        }
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
