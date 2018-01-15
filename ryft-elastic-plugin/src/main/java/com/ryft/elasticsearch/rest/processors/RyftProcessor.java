package com.ryft.elasticsearch.rest.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.ryft.elasticsearch.utils.PostConstruct;
import com.ryft.elasticsearch.plugin.disruptor.messages.RequestEvent;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.ryft.elasticsearch.plugin.ObjectMapperFactory;
import com.ryft.elasticsearch.plugin.PropertiesProvider;
import com.ryft.elasticsearch.plugin.disruptor.messages.IndexSearchRequestEvent;
import com.ryft.elasticsearch.plugin.disruptor.messages.SearchRequestEvent;
import com.ryft.elasticsearch.plugin.service.AggregationService;
import com.ryft.elasticsearch.rest.client.ClusterRestClientStreamHandler;
import com.ryft.elasticsearch.rest.client.NettyUtils;
import com.ryft.elasticsearch.rest.client.RyftRestClient;
import com.ryft.elasticsearch.rest.client.RyftSearchException;
import com.ryft.elasticsearch.rest.mappings.RyftRequestPayload;
import com.ryft.elasticsearch.rest.mappings.RyftStreamResponse;
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
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
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
                SearchResponse response = executeRequest(event);
                event.getCallback().onResponse(response);
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

    protected RyftStreamResponse sendToRyft(SearchRequestEvent requestEvent) throws RyftSearchException {
        URI ryftURI = requestEvent.getRyftSearchURL();
        HttpRequest ryftRequest = getRyftHttpRequest(requestEvent);
        LOGGER.debug("Prepared request: {}", ryftRequest);
        Optional<Channel> maybeRyftChannel = channelProvider.get(ryftURI.getHost());
        if (maybeRyftChannel.isPresent()) {
            Channel ryftChannel = maybeRyftChannel.get();
            CountDownLatch countDownLatch = new CountDownLatch(1);
            ryftChannel.pipeline().addLast(new ClusterRestClientStreamHandler(countDownLatch, requestEvent.getSize(), mapper));
            LOGGER.debug("Send request: {}", ryftRequest);
            ChannelFuture channelFuture = ryftChannel.writeAndFlush(ryftRequest);
            try {
                countDownLatch.await();
            } catch (InterruptedException ex) {
                throw new RyftSearchException(ex);
            }
            return NettyUtils.getAttribute(channelFuture.channel(), ClusterRestClientStreamHandler.RYFT_STREAM_RESPONSE_ATTR);
        } else {
            LOGGER.error("Can not connect to {}", ryftURI.getHost());
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

    protected SearchResponse constructSearchResponse(SearchRequestEvent requestEvent, RyftStreamResponse ryftResponse, Long startTime) throws RyftSearchException {
        Integer totalShards = 0;
        if (requestEvent instanceof IndexSearchRequestEvent) {
            totalShards = ((IndexSearchRequestEvent) requestEvent).getShardsNumber().intValue();
        }
        Integer failureShards = ryftResponse.getFailures().size();
        InternalAggregations aggregations = aggregationService.applyAggregation(requestEvent, ryftResponse);
        Long totalHits = new Long(ryftResponse.getSearchHits().size());
        if (ryftResponse.getStats() != null) {
            totalHits = ryftResponse.getStats().getMatches();
        }
        Integer size = requestEvent.getRequestParameters().getRyftProperties()
                .getInt(PropertiesProvider.ES_RESULT_SIZE);
        InternalSearchHit[] hits = ryftResponse.getSearchHits().stream().limit(size).toArray(InternalSearchHit[]::new);
        InternalSearchHits internalSearchHits = new InternalSearchHits(hits, totalHits, Float.NEGATIVE_INFINITY);
        InternalSearchResponse internalSearchResponse = new InternalSearchResponse(internalSearchHits, aggregations,
                null, null, false, false);
        Long searchTime = System.currentTimeMillis() - startTime;
        LOGGER.info("Search time: {} ms. Total hits: {}. Results: {}. Failures: {}", searchTime, totalHits, hits.length, failureShards);
 
        return new SearchResponse(internalSearchResponse, null, totalShards, totalShards - failureShards, searchTime,
                ryftResponse.getFailures().toArray(new ShardSearchFailure[ryftResponse.getFailures().size()]));
    }
}
