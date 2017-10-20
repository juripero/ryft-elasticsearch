package com.ryft.elasticsearch.rest.processors;

import com.ryft.elasticsearch.plugin.PropertiesProvider;
import com.ryft.elasticsearch.plugin.disruptor.messages.IndexSearchRequestEvent;
import com.ryft.elasticsearch.plugin.service.AggregationService;
import com.ryft.elasticsearch.rest.client.ClusterRestClientHandler;
import com.ryft.elasticsearch.rest.client.NettyUtils;
import com.ryft.elasticsearch.rest.client.RyftRestClient;
import com.ryft.elasticsearch.rest.client.RyftSearchException;
import com.ryft.elasticsearch.rest.mappings.RyftResponse;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.http.HttpRequest;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.search.SearchShardTarget;

public class IndexSearchRequestProcessor extends RyftProcessor<IndexSearchRequestEvent> {

    @Inject
    public IndexSearchRequestProcessor(PropertiesProvider properties,
            RyftRestClient channelProvider, AggregationService aggregationService) {
        super(properties, channelProvider, aggregationService);
    }

    @Override
    protected SearchResponse executeRequest(IndexSearchRequestEvent event) throws RyftSearchException {
        return getSearchResponse(event);
    }

    private SearchResponse getSearchResponse(IndexSearchRequestEvent requestEvent) throws RyftSearchException {
        Long start = System.currentTimeMillis();
        RyftResponse ryftResponse = sendToRyft(requestEvent);
        Long searchTime = System.currentTimeMillis() - start;
        return getSearchResponse(requestEvent, ryftResponse, searchTime);
    }

    private RyftResponse sendToRyft(IndexSearchRequestEvent requestEvent) throws RyftSearchException {
        Collection<SearchShardTarget> shardsToSearch = requestEvent.getShardsToSearch();
        HttpRequest ryftRequest = requestEvent.getRyftRequest(shardsToSearch);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Channel ryftChannel = channelProvider.get();
        ryftChannel.pipeline().addLast(new ClusterRestClientHandler(countDownLatch));
        LOGGER.debug("Send request: {}", ryftRequest);
        ChannelFuture channelFuture = ryftChannel.writeAndFlush(ryftRequest);
        try {
            countDownLatch.await();
        } catch (InterruptedException ex) {
            throw new RyftSearchException(ex);
        }
        return NettyUtils.getAttribute(channelFuture.channel(), ClusterRestClientHandler.RYFT_RESPONSE_ATTR);
    }

    private SearchShardTarget getSearchShardTarget(ShardRouting shardRouting) {
        return new SearchShardTarget(shardRouting.currentNodeId(), shardRouting.index(), shardRouting.getId());
    }

    private SearchResponse getSearchResponse(IndexSearchRequestEvent requestEvent,
            RyftResponse ryftResponse, Long searchTime) throws RyftSearchException {
        LOGGER.info("Search successful. Search time: {}", searchTime);
        SearchShardTarget searchShardTarget = requestEvent.getShardsToSearch().get(0);
        Map<SearchShardTarget, RyftResponse> resultResponses = new HashMap();
        resultResponses.put(searchShardTarget, ryftResponse);
        return constructSearchResponse(requestEvent, resultResponses, searchTime);
    }

    @Override
    public int getPoolSize() {
        return props.get().getInt(PropertiesProvider.REQ_THREAD_NUM);
    }

    @Override
    public String getName() {
        return String.format("ryft-indexsearch-pool-%d", getPoolSize());
    }

}
