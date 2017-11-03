package com.ryft.elasticsearch.rest.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.ryft.elasticsearch.utils.PostConstruct;
import com.ryft.elasticsearch.plugin.disruptor.messages.RequestEvent;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.ryft.elasticsearch.plugin.ObjectMapperFactory;
import com.ryft.elasticsearch.plugin.PropertiesProvider;
import com.ryft.elasticsearch.plugin.disruptor.messages.FileSearchRequestEvent;
import com.ryft.elasticsearch.plugin.disruptor.messages.IndexSearchRequestEvent;
import com.ryft.elasticsearch.plugin.disruptor.messages.SearchRequestEvent;
import com.ryft.elasticsearch.plugin.service.AggregationService;
import com.ryft.elasticsearch.rest.client.ClusterRestClientHandler;
import com.ryft.elasticsearch.rest.client.NettyUtils;
import com.ryft.elasticsearch.rest.client.RyftRestClient;
import com.ryft.elasticsearch.rest.client.RyftSearchException;
import com.ryft.elasticsearch.rest.mappings.RyftRequestPayload;
import com.ryft.elasticsearch.rest.mappings.RyftResponse;
import com.ryft.elasticsearch.rest.mappings.RyftResult;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
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

    protected final ESLogger LOGGER = Loggers.getLogger(this.getClass());

    protected ExecutorService executor;
    protected final RyftRestClient channelProvider;
    protected final AggregationService aggregationService;
    protected final ObjectMapper mapper;

    public RyftProcessor(ObjectMapperFactory objectMapperFactory,
            RyftRestClient channelProvider, AggregationService aggregationService) {
        mapper = objectMapperFactory.get();
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

    protected RyftResponse sendToRyft(SearchRequestEvent requestEvent) throws RyftSearchException {
        URI ryftURI = requestEvent.getRyftSearchURL();
        RyftRequestPayload payload = requestEvent.getRyftRequestPayload();
        LOGGER.info("Preparing request to {}", ryftURI.getHost());
        if (payload.getTweaks() != null) {
            LOGGER.info("Requesting routes: {}", payload.getTweaks().getClusterRoutes());
        }
        HttpRequest ryftRequest = getRyftHttpRequest(requestEvent);
        Optional<Channel> maybeRyftChannel = channelProvider.get(ryftURI.getHost());
        if (maybeRyftChannel.isPresent()) {
            Channel ryftChannel = maybeRyftChannel.get();
            CountDownLatch countDownLatch = new CountDownLatch(1);
            ryftChannel.pipeline().addLast(new ClusterRestClientHandler(countDownLatch));
            LOGGER.debug("Send request: {}", ryftRequest);
            ChannelFuture channelFuture = ryftChannel.writeAndFlush(ryftRequest);
            try {
                countDownLatch.await();
            } catch (InterruptedException ex) {
                throw new RyftSearchException(ex);
            }
            return NettyUtils.getAttribute(channelFuture.channel(), ClusterRestClientHandler.RYFT_RESPONSE_ATTR);
        } else {
            requestEvent.addFailedNode(ryftURI.getHost());
            return sendToRyft(requestEvent);
        }
    }

    public HttpRequest getRyftHttpRequest(SearchRequestEvent requestEvent) throws RyftSearchException {
        requestEvent.validateRequest();
        URI ryftURI = requestEvent.getRyftSearchURL();
        RyftRequestPayload payload = requestEvent.getRyftRequestPayload();
        LOGGER.info("Preparing request to {}", ryftURI.getHost());
        if (payload.getTweaks() != null) {
            LOGGER.info("Requesting routes: {}", payload.getTweaks().getClusterRoutes());
        }
        try {
            DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, ryftURI.toString());
            String body = mapper.writeValueAsString(payload);
            ByteBuf bbuf = Unpooled.copiedBuffer(body, StandardCharsets.UTF_8);
            request.headers().add(HttpHeaders.Names.HOST, String.format("%s:%d", ryftURI.getHost(), ryftURI.getPort()));
            request.headers().set(HttpHeaders.Names.CONTENT_LENGTH, bbuf.readableBytes());
            request.headers().set(HttpHeaders.Names.CONTENT_TYPE, "application/json");
            if (requestEvent.getRequestParameters().getRyftProperties().getBool(PropertiesProvider.RYFT_REST_AUTH_ENABLED)) {
                String login = requestEvent.getRequestParameters().getRyftProperties().getStr(PropertiesProvider.RYFT_REST_LOGIN);
                String password = requestEvent.getRequestParameters().getRyftProperties().getStr(PropertiesProvider.RYFT_REST_PASSWORD);
                String basicAuthToken = Base64.getEncoder().encodeToString(String.format("%s:%s", login, password).getBytes());
                request.headers().add(HttpHeaders.Names.AUTHORIZATION, "Basic " + basicAuthToken);
            }
            request.content().clear().writeBytes(bbuf);
            return request;
        } catch (JsonProcessingException ex) {
            throw new RyftSearchException("Ryft search URL composition exception", ex);
        }
    }

    protected SearchResponse constructSearchResponse(SearchRequestEvent requestEvent, RyftResponse ryftResponse, Long searchTime) throws RyftSearchException {
        List<InternalSearchHit> searchHitList = new ArrayList<>();
        List<ShardSearchFailure> failures = new ArrayList<>();
        Integer totalShards = 0;
        if (requestEvent instanceof IndexSearchRequestEvent) {
            totalShards = ((IndexSearchRequestEvent) requestEvent).getShardsNumber().intValue();
        }
        Integer failureShards = 0;
        String errorMessage = ryftResponse.getMessage();
        List<String> errors = ryftResponse.getErrorsAndMessage();

        if (ryftResponse.hasErrors()) {
            failureShards += 1;
            if ((errorMessage != null) && (!errorMessage.isEmpty())) {
                failures.add(new ShardSearchFailure(new Exception(errorMessage)));
            }
            if ((errors != null) && (!errors.isEmpty())) {
                errors.stream()
                        .map(error -> new ShardSearchFailure(new Exception(error)))
                        .collect(Collectors.toCollection(() -> failures));
            }
        }

        if (ryftResponse.hasResults()) {
            ryftResponse.getResults().stream().map(
                    result -> processSearchResult(result, null)
            ).collect(Collectors.toCollection(() -> searchHitList));
        }

        LOGGER.info("Search time: {} ms. Results: {}. Failures: {}", searchTime, searchHitList.size(), failures.size());
        InternalAggregations aggregations;
        if (!requestEvent.canBeAggregatedByRYFT()) {
            aggregations = aggregationService.applyAggregationElastic(searchHitList, requestEvent);
        } else {
            aggregations = aggregationService.applyAggregationRyft(requestEvent, ryftResponse);
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

    protected InternalSearchHit processSearchResult(RyftResult hit, SearchShardTarget searchShardTarget) {
        LOGGER.debug("Processing search result: {}", hit);
        InternalSearchHit searchHit;
        try {
            String uid = hit.record().has("_uid") ? hit.record().get("_uid").asText() : String.valueOf(hit.hashCode());
            String type = hit.record().has("type") ? hit.record().get("type").asText() : FileSearchRequestEvent.NON_INDEXED_TYPE;

            searchHit = new InternalSearchHit(0, uid, new Text(type),
                    ImmutableMap.of());
            searchHit.shardTarget(searchShardTarget);

            String error = hit.record().has("error") ? hit.record().get("error").asText() : "";
            if (!error.isEmpty()) {
                searchHit.sourceRef(new BytesArray("{\"error\": \"" + error + "\"}"));
            } else {
                hit.record().remove("_index");
                hit.record().remove("_uid");
                hit.record().remove("type");
                searchHit.sourceRef(new BytesArray(hit.record().toString()));
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
