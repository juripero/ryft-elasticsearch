package com.ryft.elasticsearch.rest.processors;

import com.ryft.elasticsearch.plugin.ObjectMapperFactory;
import com.ryft.elasticsearch.plugin.PropertiesProvider;
import com.ryft.elasticsearch.plugin.disruptor.messages.FileSearchRequestEvent;
import com.ryft.elasticsearch.plugin.service.AggregationService;
import com.ryft.elasticsearch.rest.client.RyftRestClient;
import com.ryft.elasticsearch.rest.client.RyftSearchException;
import com.ryft.elasticsearch.rest.mappings.RyftStreamResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.inject.Inject;

public class FileSearchRequestProcessor extends RyftProcessor<FileSearchRequestEvent> {

    private final PropertiesProvider props;

    @Inject
    public FileSearchRequestProcessor(PropertiesProvider properties,
            ObjectMapperFactory objectMapperFactory, RyftRestClient channelProvider,
            AggregationService aggregationService) {
        super(objectMapperFactory, channelProvider, aggregationService);
        this.props = properties;
    }

    @Override
    protected SearchResponse executeRequest(FileSearchRequestEvent event) throws RyftSearchException {
        Long start = System.currentTimeMillis();
        RyftStreamResponse ryftResponse = sendToRyft(event);
        return constructSearchResponse(event, ryftResponse, start);
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
