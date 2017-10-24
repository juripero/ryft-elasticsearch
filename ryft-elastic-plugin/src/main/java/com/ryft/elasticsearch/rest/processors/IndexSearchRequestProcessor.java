package com.ryft.elasticsearch.rest.processors;

import com.ryft.elasticsearch.plugin.PropertiesProvider;
import com.ryft.elasticsearch.plugin.disruptor.messages.IndexSearchRequestEvent;
import com.ryft.elasticsearch.plugin.service.AggregationService;
import com.ryft.elasticsearch.rest.client.RyftRestClient;
import com.ryft.elasticsearch.rest.client.RyftSearchException;
import com.ryft.elasticsearch.rest.mappings.RyftResponse;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
        RyftResponse ryftResponse;
        ryftResponse = sendToRyft(requestEvent);
        if (ryftResponse.hasErrors()) {
            LOGGER.warn("RYFT response has errors: {}", Arrays.toString(ryftResponse.getErrors()));
            List<String> failedNodes = getFailedNodes(ryftResponse);
            failedNodes.forEach(requestEvent::addFailedNode);
            return getSearchResponse(requestEvent);
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

    private List<String> getFailedNodes(RyftResponse ryftResponse) {
        List<String> result = new ArrayList<>();
        Pattern addressPattern = Pattern.compile("\\(CLUSTER\\{.*?addr:(.*?)\\}\\)");
        for (String error : ryftResponse.getErrors()) {
            Matcher matcher = addressPattern.matcher(error);
            if (matcher.find()) {
                try {
                    URL url = new URL(matcher.group(1));
                    result.add(url.getHost());
                } catch (MalformedURLException | RuntimeException ex) {
                    LOGGER.warn("can not extract failed node from errormessage.");
                }
            }
        }
        return result;
    }

}
