package com.ryft.elasticsearch.rest.processors;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.ryft.elasticsearch.utils.PostConstruct;
import com.ryft.elasticsearch.plugin.disruptor.messages.RequestEvent;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.ryft.elasticsearch.plugin.PropertiesProvider;
import com.ryft.elasticsearch.plugin.disruptor.messages.FileSearchRequestEvent;
import com.ryft.elasticsearch.plugin.disruptor.messages.SearchRequestEvent;
import com.ryft.elasticsearch.plugin.service.AggregationService;
import com.ryft.elasticsearch.rest.client.ClusterRestClientHandler;
import com.ryft.elasticsearch.rest.client.NettyUtils;
import com.ryft.elasticsearch.rest.client.RyftRestClient;
import com.ryft.elasticsearch.rest.client.RyftSearchException;
import com.ryft.elasticsearch.rest.mappings.RyftResponse;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.net.URI;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;

public abstract class RyftProcessor<T extends RequestEvent> implements PostConstruct {

    private static final ESLogger LOGGER = Loggers.getLogger(RyftProcessor.class);

    protected ExecutorService executor;
    protected final RyftRestClient channelProvider;
    protected final PropertiesProvider props;
    protected final AggregationService aggregationService;

    public RyftProcessor(PropertiesProvider properties, RyftRestClient channelProvider,
            AggregationService aggregationService) {
        this.props = properties;
        this.channelProvider = channelProvider;
        this.aggregationService = aggregationService;
    }

    @Override
    public void onPostConstruct() {
        executor = Executors
                .newFixedThreadPool(getPoolSize(), new ThreadFactoryBuilder().setNameFormat(getName()).build());
    }

    public void process(T event) {
        LOGGER.info("Processing event: {}", event);
        executor.submit(() -> {
            try {
                event.getCallback().onResponse(executeRequest(event));
            } catch (RyftSearchException | RuntimeException ex) {
                LOGGER.error("Request processing error", ex);
                event.getCallback().onFailure(ex);
            }
        });
    }

    protected abstract SearchResponse executeRequest(T event) throws RyftSearchException;

    /**
     * Should return name for current pool impl
     *
     * @return
     */
    public abstract String getName();

    public abstract int getPoolSize();

    protected Optional<ChannelFuture> sendToRyft(URI searchUri, ShardRouting shardRouting, CountDownLatch countDownLatch) {
        LOGGER.info("Search in shard: {}", shardRouting);
        return channelProvider.get(searchUri.getHost()).map((ryftChannel) -> {
            NettyUtils.setAttribute(ClusterRestClientHandler.INDEX_SHARD_ATTR, shardRouting, ryftChannel);
            ryftChannel.pipeline().addLast(new ClusterRestClientHandler(countDownLatch));
            DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, searchUri.toString());

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

    protected SearchResponse constructSearchResponse(SearchRequestEvent requestEvent, Map<SearchShardTarget, RyftResponse> resultResponses, Long searchTime) throws RyftSearchException {
        List<InternalSearchHit> searchHitList = new ArrayList<>();
        List<ShardSearchFailure> failures = new ArrayList<>();
        Integer totalShards = 0;
        Integer failureShards = 0;
        for (Map.Entry<SearchShardTarget, RyftResponse> entry : resultResponses.entrySet()) {
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
                ryftResponse.getResults().stream().map(
                        result -> processSearchResult(result, searchShardTarget)
                ).collect(Collectors.toCollection(() -> searchHitList));
            }
        }
        LOGGER.info("Search time: {} ms. Results: {}. Failures: {}", searchTime, searchHitList.size(), failures.size());
        InternalAggregations aggregations;
        if (requestEvent.getRyftSupportedAggregationQuery() == null) {
            aggregations = aggregationService.applyAggregationElastic(searchHitList, requestEvent);
        } else {
            aggregations = aggregationService.applyAggregationRyft(requestEvent);
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

    protected InternalSearchHit processSearchResult(ObjectNode hit, SearchShardTarget searchShardTarget) {
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

}
