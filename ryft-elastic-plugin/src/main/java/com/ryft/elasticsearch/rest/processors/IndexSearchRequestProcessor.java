package com.ryft.elasticsearch.rest.processors;

import com.ryft.elasticsearch.plugin.ObjectMapperFactory;
import com.ryft.elasticsearch.plugin.PropertiesProvider;
import com.ryft.elasticsearch.plugin.disruptor.messages.IndexSearchRequestEvent;
import com.ryft.elasticsearch.plugin.service.AggregationService;
import com.ryft.elasticsearch.rest.client.RyftRestClient;
import com.ryft.elasticsearch.rest.client.RyftSearchException;
import com.ryft.elasticsearch.rest.mappings.RyftResponse;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.inject.Inject;

public class IndexSearchRequestProcessor extends RyftProcessor<IndexSearchRequestEvent> {

    private final PropertiesProvider props;

    @Inject
    public IndexSearchRequestProcessor(PropertiesProvider properties,
            ObjectMapperFactory objectMapperFactory, RyftRestClient channelProvider,
            AggregationService aggregationService) {
        super(objectMapperFactory, channelProvider, aggregationService);
        this.props = properties;
    }

    @Override
    protected SearchResponse executeRequest(IndexSearchRequestEvent event) throws RyftSearchException {
        return getSearchResponse(event, new ArrayList<>(), null, 0);
    }

    private SearchResponse getSearchResponse(IndexSearchRequestEvent requestEvent,
            List<RyftResponse> responseHistory, Long start, Integer count) throws RyftSearchException {
        if (start == null) {
            start = System.currentTimeMillis();
        }
        if (requestEvent.canBeExecuted()) {
            RyftResponse ryftResponse = sendToRyft(requestEvent);
            responseHistory.add(ryftResponse);
            if (ryftResponse.hasErrors() && 
                    (count < requestEvent.getClusterService().state().getNodes().size())) {
                LOGGER.warn("RYFT response has errors: {}", ryftResponse);
                List<String> failedNodes = getFailedNodes(ryftResponse);
                failedNodes.forEach(requestEvent::addFailedNode);
                return getSearchResponse(requestEvent, responseHistory, start, ++count);
            }
        }
        if (responseHistory.isEmpty()) {
            throw new RyftSearchException("Can not get any RYFT response");
        }
        RyftResponse maxResponse = responseHistory.stream()
                .max((r1, r2) -> r1.getResults().size() - r2.getResults().size()).get();
        Long searchTime = System.currentTimeMillis() - start;
        return constructSearchResponse(requestEvent, maxResponse, searchTime);
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
        for (String error : ryftResponse.getErrorsAndMessage()) {
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
