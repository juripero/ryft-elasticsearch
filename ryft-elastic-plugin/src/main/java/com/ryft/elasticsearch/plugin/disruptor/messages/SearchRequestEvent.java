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
package com.ryft.elasticsearch.plugin.disruptor.messages;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ryft.elasticsearch.converter.entities.RyftRequestParameters;
import com.ryft.elasticsearch.rest.client.RyftSearchException;
import com.ryft.elasticsearch.converter.ryftdsl.RyftFormat;
import com.ryft.elasticsearch.plugin.ObjectMapperFactory;
import com.ryft.elasticsearch.plugin.PropertiesProvider;
import com.ryft.elasticsearch.plugin.RyftProperties;
import com.ryft.elasticsearch.rest.mappings.RyftRequestPayload;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;

public abstract class SearchRequestEvent extends RequestEvent {

    protected final ClusterService clusterService;

    protected final RyftRequestParameters requestParameters;

    protected String ryftSupportedAggregationQuery;

    private final List<String> supportedAggregations;

    protected final ObjectMapper mapper;

    protected final List<String> nodesToSearch;
    
    private static final String LOCALHOST = "127.0.0.1";

    @Inject
    protected SearchRequestEvent(ClusterService clusterService,
            ObjectMapperFactory objectMapperFactory,
            @Assisted RyftRequestParameters requestParameters) {
        super();
        this.requestParameters = requestParameters;
        this.clusterService = clusterService;
        mapper = objectMapperFactory.get();
        supportedAggregations = Arrays.asList(requestParameters.getRyftProperties().getStr(PropertiesProvider.AGGREGATIONS_ON_RYFT_SERVER).split(","));
        nodesToSearch = new ArrayList<>();
        Iterator<DiscoveryNode> nodeIterator = clusterService.state().getNodes().iterator();
        while (nodeIterator.hasNext()) {
            DiscoveryNode discoveryNode = nodeIterator.next();
            nodesToSearch.add(discoveryNode.address().getHost());
        }

    }

    public void validateRequest() throws RyftSearchException {
        RyftProperties ryftProperties = requestParameters.getRyftProperties();
        if (ryftProperties.containsKey(PropertiesProvider.RYFT_FORMAT)
                && ryftProperties.get(PropertiesProvider.RYFT_FORMAT).equals(RyftFormat.UNKNOWN_FORMAT)) {
            throw new RyftSearchException("Unknown format. Please use one of the following formats: json, xml, utf8, raw");
        }
    }

    public abstract RyftRequestPayload getRyftRequestPayload() throws RyftSearchException;

    public abstract URI getRyftSearchURL() throws RyftSearchException;

    protected String getEncodedQuery() throws RyftSearchException {
        try {
            return URLEncoder.encode(
                    this.requestParameters.getQuery().buildRyftString(), "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            throw new RyftSearchException(ex);
        }
    }

    public Integer getSize() {
        if (canBeAggregatedByRyft()) {
            return requestParameters.getRyftProperties()
                    .getInt(PropertiesProvider.ES_RESULT_SIZE);
        } else {
            return -1;
        }
    }

    public RyftProperties getMapping() {
        return requestParameters.getRyftProperties()
                .getRyftProperties(PropertiesProvider.RYFT_MAPPING);
    }

    protected RyftFormat getFormat() {
        return (RyftFormat) requestParameters.getRyftProperties()
                .get(PropertiesProvider.RYFT_FORMAT);
    }

    protected Boolean getCaseSensitive() {
        return requestParameters.getRyftProperties()
                .getBool(PropertiesProvider.RYFT_CASE_SENSITIVE);
    }

    public ObjectNode getParsedQuery() {
        return requestParameters.getParsedQuery();
    }

    public boolean canBeAggregatedByRyft() {
        if (!getParsedQuery().has("aggs") && !getParsedQuery().has("aggregations")) {
            return true;
        } else {
            ObjectNode aggregations = getAggregations();
            if (aggregations != null) {
                if (aggregations.findValue("script") != null) {
                    return false;
                }
                if (aggregations.findValue("meta") != null) {
                    return false;
                }
                Boolean result = true;
                for (JsonNode aggregation : aggregations) {
                    result &= supportedAggregations.contains(aggregation.fieldNames().next());
                }
                return result;
            } else {
                return true;
            }
        }
    }

    protected ObjectNode getAggregations() {
        if (getParsedQuery().has("aggs")) {
            return (ObjectNode) getParsedQuery().findValue("aggs");
        } else {
            return (ObjectNode) getParsedQuery().findValue("aggregations");
        }
    }

    protected String getHost() {
        if (nodesToSearch.contains(clusterService.localNode().address().getHost())) {
            return LOCALHOST;
        } else {
            return nodesToSearch.get(0);
        }
    }

    protected String getPort() {
        return requestParameters.getRyftProperties().getStr(PropertiesProvider.PORT);
    }

    public RyftRequestParameters getRequestParameters() {
        return requestParameters;
    }

    public void addFailedNode(String hostname) {
        if (hostname.equals(LOCALHOST)) {
            nodesToSearch.remove(clusterService.localNode().address().getHost());
        } else {
            nodesToSearch.remove(hostname);
        }
    }

    public ClusterService getClusterService() {
        return clusterService;
    }

}
