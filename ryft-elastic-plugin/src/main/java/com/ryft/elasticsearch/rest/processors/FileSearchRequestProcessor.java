package com.ryft.elasticsearch.rest.processors;

import com.ryft.elasticsearch.plugin.PropertiesProvider;
import com.ryft.elasticsearch.plugin.disruptor.messages.FileSearchRequestEvent;
import com.ryft.elasticsearch.plugin.service.AggregationService;
import com.ryft.elasticsearch.rest.client.ClusterRestClientHandler;
import com.ryft.elasticsearch.rest.client.NettyUtils;
import com.ryft.elasticsearch.rest.client.RyftRestClient;
import com.ryft.elasticsearch.rest.client.RyftSearchException;
import com.ryft.elasticsearch.rest.mappings.RyftResponse;
import io.netty.channel.ChannelFuture;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.search.SearchShardTarget;

public class FileSearchRequestProcessor extends RyftProcessor<FileSearchRequestEvent> {

    private static final ESLogger LOGGER = Loggers.getLogger(FileSearchRequestProcessor.class);

    @Inject
    public FileSearchRequestProcessor(PropertiesProvider properties, 
            RyftRestClient channelProvider, AggregationService aggregationService) {
        super(properties, channelProvider, aggregationService);
    }

    @Override
    protected SearchResponse executeRequest(FileSearchRequestEvent event) throws RyftSearchException {
        Long start = System.currentTimeMillis();
        Map<SearchShardTarget, RyftResponse> resultResponses = sendToRyft(event);
        Long searchTime = System.currentTimeMillis() - start;
        return constructSearchResponse(event, resultResponses, searchTime);
    }

    private Map<SearchShardTarget, RyftResponse> sendToRyft(FileSearchRequestEvent requestEvent) throws RyftSearchException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Optional<ChannelFuture> maybeChannelFuture = sendToRyft(requestEvent.getRyftSearchURL(), null, countDownLatch);
        try {
            countDownLatch.await();
        } catch (InterruptedException ex) {
            throw new RyftSearchException(ex);
        }
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

    @Override
    public int getPoolSize() {
        return props.get().getInt(PropertiesProvider.REQ_THREAD_NUM);
    }

    @Override
    public String getName() {
        return String.format("ryft-filesearch-pool-%d", getPoolSize());
    }

}
