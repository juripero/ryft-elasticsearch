/**
 * ##License
 * Ryft-Customized BSD License
 * Copyright (c) 2018, Ryft Systems, Inc.
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation and/or
 *   other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software must display the following acknowledgement:
 *   This product includes software developed by Ryft Systems, Inc.
 * 4. Neither the name of Ryft Systems, Inc. nor the names of its contributors may be used
 *   to endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY RYFT SYSTEMS, INC. ''AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL RYFT SYSTEMS, INC. BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
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
