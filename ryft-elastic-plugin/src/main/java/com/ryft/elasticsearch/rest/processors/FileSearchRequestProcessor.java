package com.ryft.elasticsearch.rest.processors;

import com.ryft.elasticsearch.plugin.PropertiesProvider;
import com.ryft.elasticsearch.plugin.disruptor.messages.FileSearchRequestEvent;
import com.ryft.elasticsearch.plugin.service.AggregationService;
import com.ryft.elasticsearch.rest.client.ClusterRestClientHandler;
import com.ryft.elasticsearch.rest.client.NettyUtils;
import com.ryft.elasticsearch.rest.client.RyftRestClient;
import com.ryft.elasticsearch.rest.client.RyftSearchException;
import com.ryft.elasticsearch.rest.mappings.RyftResponse;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.http.HttpRequest;
import java.util.concurrent.CountDownLatch;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.inject.Inject;

public class FileSearchRequestProcessor extends RyftProcessor<FileSearchRequestEvent> {

    @Inject
    public FileSearchRequestProcessor(PropertiesProvider properties,
            RyftRestClient channelProvider, AggregationService aggregationService) {
        super(properties, channelProvider, aggregationService);
    }

    @Override
    protected SearchResponse executeRequest(FileSearchRequestEvent event) throws RyftSearchException {
        Long start = System.currentTimeMillis();
        RyftResponse ryftResponse = sendToRyft(event);
        Long searchTime = System.currentTimeMillis() - start;
        return constructSearchResponse(event, ryftResponse, searchTime);
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
