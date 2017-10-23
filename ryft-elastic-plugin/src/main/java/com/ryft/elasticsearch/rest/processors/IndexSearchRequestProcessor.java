package com.ryft.elasticsearch.rest.processors;

import com.ryft.elasticsearch.plugin.PropertiesProvider;
import com.ryft.elasticsearch.plugin.disruptor.messages.IndexSearchRequestEvent;
import com.ryft.elasticsearch.plugin.service.AggregationService;
import com.ryft.elasticsearch.rest.client.RyftRestClient;
import com.ryft.elasticsearch.rest.client.RyftSearchException;
import com.ryft.elasticsearch.rest.mappings.RyftResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.inject.Inject;

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
        if (ryftResponse.hasErrors()) {
            LOGGER.warn("RYFT error: {}", ryftResponse.toString());
        }
        Long searchTime = System.currentTimeMillis() - start;
        return constructSearchResponse(requestEvent, ryftResponse, searchTime);
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
