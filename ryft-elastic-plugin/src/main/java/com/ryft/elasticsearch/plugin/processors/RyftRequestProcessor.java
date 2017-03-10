package com.ryft.elasticsearch.plugin.processors;

import com.google.common.collect.ImmutableMap;
import io.netty.channel.Channel;
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
import java.util.ArrayList;
import java.util.List;
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
        executor.submit(() -> sendToRyftCluster(requestEvent));
    }

    protected void sendToRyftCluster(RyftClusterRequestEvent requestEvent) {
        try {
            List<ShardRouting> shardRoutings = requestEvent.getShards();
            List<ShardRouting> primaryShardsRoutings = shardRoutings.stream()
                    .filter(shard -> shard.primary())
                    .collect(Collectors.toList());
            List<ChannelFuture> channelFutures = new ArrayList<>();
            CountDownLatch countDownLatch = new CountDownLatch(primaryShardsRoutings.size());
            Long start = System.currentTimeMillis();
            for (ShardRouting shardRouting : primaryShardsRoutings) {
                URI uri = requestEvent.getRyftSearchURL(shardRouting);
                channelFutures.add(sendToRyft(uri, shardRouting, countDownLatch));
            }
            countDownLatch.await();
            Long diff = System.currentTimeMillis() - start;
            SearchResponse searchResponse = getSearchResponse(channelFutures, diff);
            requestEvent.getCallback().onResponse(searchResponse);
        } catch (Exception ex) {
            LOGGER.error("Failed to connect to ryft server", ex);
            requestEvent.getCallback().onFailure(ex);
        }
    }

    protected ChannelFuture sendToRyft(URI searchUri, ShardRouting shardRouting, CountDownLatch countDownLatch) throws InterruptedException {
        LOGGER.info("URI: {}", searchUri);
        Channel ryftChannel = channelProvider.get(searchUri.getHost());
        NettyUtils.setAttribute(ClusterRestClientHandler.INDEX_SHARD_ATTR, shardRouting, ryftChannel);
        ryftChannel.pipeline().addLast(new ClusterRestClientHandler(countDownLatch));
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, searchUri.toString());
        request.headers().add(HttpHeaders.Names.AUTHORIZATION, "Basic " + props.get().getStr(PropertiesProvider.RYFT_REST_AUTH));
        request.headers().add(HttpHeaders.Names.HOST, String.format("%s:%d", searchUri.getHost(), searchUri.getPort()));
        LOGGER.info("Send request: {}", request);
        return ryftChannel.writeAndFlush(request);
    }

    private SearchResponse getSearchResponse(List<ChannelFuture> channelFutures, Long diff) {
        List<InternalSearchHit> searchHits = new ArrayList<>();
        List<ShardSearchFailure> failures = new ArrayList<>();
        Long totalHits = 0l;
        Integer totalShards = 0;
        Integer failureShards = 0;

        for (ChannelFuture channelFuture : channelFutures) {
            totalShards += 1;
            RyftResponse ryftResponse = NettyUtils.getAttribute(channelFuture.channel(), ClusterRestClientHandler.RYFT_RESPONSE_ATTR);
            ShardRouting indexShard = NettyUtils.getAttribute(channelFuture.channel(), ClusterRestClientHandler.INDEX_SHARD_ATTR);
            String errorMessage = ryftResponse.getMessage();
            String[] errors = ryftResponse.getErrors();
            SearchShardTarget searchShardTarget = getSearchShardTarget(indexShard);
            if ((errorMessage != null) && (!errorMessage.isEmpty())) {
                failureShards += 1;
                failures.add(new ShardSearchFailure(new Exception(errorMessage), searchShardTarget));
            } else {
                if (errors != null) {
                    failureShards += 1;
                    Stream.of(errors)
                            .map(error -> new ShardSearchFailure(new Exception(error), searchShardTarget))
                            .collect(Collectors.toCollection(() -> failures));
                }
                if (ryftResponse.getResults() != null) {
                    ryftResponse.getResults().forEach(hit -> {
                        String uid = hit.has("_uid") ? hit.get("_uid").asText() : UUID.randomUUID().toString();
                        String type = hit.has("type") ? hit.get("type").asText() : "";

                        InternalSearchHit searchHit = new InternalSearchHit(searchHits.size(), uid, new Text(type),
                                ImmutableMap.of());
                        searchHit.shardTarget(searchShardTarget);

                        String error = hit.has("error") ? hit.get("error").asText() : "";
                        if (!error.isEmpty()) {
                            searchHit.sourceRef(new BytesArray("{\"error\": \"" + error + "\"}"));
                        } else {
                            hit.remove("_index");
                            searchHit.sourceRef(new BytesArray(hit.toString()));
                        }
                        searchHits.add(searchHit);
                    });
                }
                if ((ryftResponse.getStats() != null) && (ryftResponse.getStats().getMatches() != null)) {
                    totalHits += ryftResponse.getStats().getMatches();
                }
            }
        }

        InternalSearchHits hits = new InternalSearchHits(
                searchHits.toArray(new InternalSearchHit[searchHits.size()]),
                totalHits == 0 ? searchHits.size() : totalHits, 1.0f);

        InternalSearchResponse internalSearchResponse = new InternalSearchResponse(hits, InternalAggregations.EMPTY,
                null, null, false, false);

        SearchResponse searchResponse = new SearchResponse(internalSearchResponse, null, totalShards, totalShards - failureShards, diff,
                failures.toArray(new ShardSearchFailure[failures.size()]));

        return searchResponse;
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
