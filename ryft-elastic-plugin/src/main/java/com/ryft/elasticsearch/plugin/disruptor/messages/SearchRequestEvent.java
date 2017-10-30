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

    protected long requestId;

    private final List<String> supportedAggregations;

    protected final ObjectMapper mapper;

    protected final List<String> nodesToSearch;

    @Inject
    protected SearchRequestEvent(ClusterService clusterService,
            ObjectMapperFactory objectMapperFactory,
            @Assisted RyftRequestParameters requestParameters) {
        super();
        this.requestParameters = requestParameters;
        this.clusterService = clusterService;
        requestId = System.currentTimeMillis();
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

    protected Integer getLimit() {
        return requestParameters.getRyftProperties()
                .getInt(PropertiesProvider.RYFT_QUERY_LIMIT);
    }

    public Integer getSize() {
        return requestParameters.getRyftProperties()
                .getInt(PropertiesProvider.ES_RESULT_SIZE);
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

    public boolean canBeAggregatedByRYFT() {
        if (!getParsedQuery().has("aggs") && !getParsedQuery().has("aggregations")) {
            return false;
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
                return false;
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

    public long getRequestId() {
        return requestId;
    }

    public String getPort() {
        return requestParameters.getRyftProperties().getStr(PropertiesProvider.PORT);
    }

    public RyftRequestParameters getRequestParameters() {
        return requestParameters;
    }

    public void addFailedNode(String hostname) {
        nodesToSearch.remove(hostname);
    }

    public ClusterService getClusterService() {
        return clusterService;
    }

}
