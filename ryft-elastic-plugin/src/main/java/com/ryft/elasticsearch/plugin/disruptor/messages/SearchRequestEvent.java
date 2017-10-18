package com.ryft.elasticsearch.plugin.disruptor.messages;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ryft.elasticsearch.converter.entities.RyftRequestParameters;
import com.ryft.elasticsearch.rest.client.RyftSearchException;
import com.ryft.elasticsearch.converter.ryftdsl.RyftFormat;
import com.ryft.elasticsearch.plugin.PropertiesProvider;
import com.ryft.elasticsearch.plugin.RyftProperties;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;

public abstract class SearchRequestEvent extends RequestEvent {

    protected final ClusterState clusterState;

    protected final RyftRequestParameters requestParameters;

    protected String ryftSupportedAggregationQuery;
    
    protected long requestId;

    @Inject
    protected SearchRequestEvent(ClusterService clusterService,
            @Assisted RyftRequestParameters requestParameters) throws RyftSearchException {
        super();
        this.requestParameters = requestParameters;
        this.clusterState = clusterService.state();
        this.requestId = System.currentTimeMillis();
    }

    protected void validateRequest() throws RyftSearchException {
        RyftProperties ryftProperties = requestParameters.getRyftProperties();
        if (ryftProperties.containsKey(PropertiesProvider.RYFT_FORMAT)
                && ryftProperties.get(PropertiesProvider.RYFT_FORMAT).equals(RyftFormat.UNKNOWN_FORMAT)) {
            throw new RyftSearchException("Unknown format. Please use one of the following formats: json, xml, utf8, raw");
        }
    }

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

    public String getRyftSupportedAggregationQuery() {
        return ryftSupportedAggregationQuery;
    }

    public void setRyftSupportedAggregationQuery(String aggregationQuery) {
        this.ryftSupportedAggregationQuery = aggregationQuery;
    }

    public long getRequestId() {
        return requestId;
    }

    public String getPort() {
        return requestParameters.getRyftProperties().getStr(PropertiesProvider.PORT);
    }
}
