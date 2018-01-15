package com.ryft.elasticsearch.rest.processors;

import com.ryft.elasticsearch.plugin.ObjectMapperFactory;
import com.ryft.elasticsearch.plugin.PropertiesProvider;
import com.ryft.elasticsearch.plugin.disruptor.messages.IndexSearchRequestEvent;
import com.ryft.elasticsearch.plugin.service.AggregationService;
import com.ryft.elasticsearch.rest.client.RyftRestClient;
import com.ryft.elasticsearch.rest.client.RyftSearchException;
import com.ryft.elasticsearch.rest.mappings.RyftStreamResponse;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
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
            List<RyftStreamResponse> responseHistory, Long start, Integer count) throws RyftSearchException {
        if (start == null) {
            start = System.currentTimeMillis();
        }
        if (requestEvent.canBeExecuted()) {
            RyftStreamResponse ryftResponse = sendToRyft(requestEvent);
            LOGGER.debug("Receive response: ", ryftResponse);
            responseHistory.add(ryftResponse);
            if (!ryftResponse.getFailures().isEmpty()
                    && (count < requestEvent.getClusterService().state().getNodes().size())) {
                LOGGER.warn("RYFT response has errors: {}", ryftResponse);
                List<String> failedNodes = getFailedNodes(ryftResponse);
                failedNodes.forEach(requestEvent::addFailedNode);
                return getSearchResponse(requestEvent, responseHistory, start, ++count);
            }
        }
        if (responseHistory.isEmpty()) {
            throw new RyftSearchException("Can not get any RYFT response");
        }
        RyftStreamResponse maxResponse = responseHistory.stream()
                .max((r1, r2)
                        -> r1.getSearchHits().size() - r2.getSearchHits().size() - r1.getFailures().size() + r2.getFailures().size()).get();
        return constructSearchResponse(requestEvent, maxResponse, start);
    }

    @Override
    public int getPoolSize() {
        return props.get().getInt(PropertiesProvider.REQ_THREAD_NUM);
    }

    @Override
    public String getName() {
        return String.format("ryft-indexsearch-pool-%d", getPoolSize());
    }

    private List<String> getFailedNodes(RyftStreamResponse ryftResponse) {
        List<String> result = new ArrayList<>();
        Pattern addressPattern = Pattern.compile("\\(CLUSTER\\{.*?addr:(.*?)\\}\\)");
        for (ShardSearchFailure error : ryftResponse.getFailures()) {
            Matcher matcher = addressPattern.matcher(error.reason());
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
