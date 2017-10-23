package com.ryft.elasticsearch.rest.processors;

import com.ryft.elasticsearch.plugin.PropertiesProvider;
import com.ryft.elasticsearch.plugin.disruptor.messages.SearchRequestEvent;
import com.ryft.elasticsearch.plugin.service.AggregationService;
import com.ryft.elasticsearch.rest.client.RyftRestClient;
import com.ryft.elasticsearch.rest.client.RyftSearchException;
import com.ryft.elasticsearch.rest.mappings.RyftResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.inject.Inject;

public class RyftSearchRequestProcessor extends RyftProcessor<SearchRequestEvent> {

    @Inject
    public RyftSearchRequestProcessor(PropertiesProvider properties, RyftRestClient channelProvider,
            AggregationService aggregationService) {
        super(properties, channelProvider, aggregationService);
    }

    @Override
    protected SearchResponse executeRequest(SearchRequestEvent event) throws RyftSearchException {
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
        return String.format("ryft-search-pool-%d", getPoolSize());
    }

}
