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

    protected final RyftProperties ryftProperties;

    protected final String query;
    protected final String encodedQuery;

    protected final ObjectNode parsedQuery;

    @Inject
    protected SearchRequestEvent(ClusterService clusterService,
            @Assisted RyftRequestParameters requestParameters) throws RyftSearchException {
        super();
        this.clusterState = clusterService.state();
        this.ryftProperties = requestParameters.getRyftProperties();
        this.query = requestParameters.getQuery().buildRyftString();
        this.parsedQuery = requestParameters.getParsedQuery();
        try {
            this.encodedQuery = URLEncoder.encode(this.query, "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            throw new RyftSearchException(ex);
        }
    }

    protected void validateRequest() throws RyftSearchException {
        if (ryftProperties.containsKey(PropertiesProvider.RYFT_FORMAT)
                && ryftProperties.get(PropertiesProvider.RYFT_FORMAT).equals(RyftFormat.UNKNOWN_FORMAT)) {
            throw new RyftSearchException("Unknown format. Please use one of the following formats: json, xml, utf8, raw");
        }
    }

    protected Integer getLimit() {
        return ryftProperties.getInt(PropertiesProvider.RYFT_QUERY_LIMIT);
    }

    public Integer getSize() {
        return ryftProperties.getInt(PropertiesProvider.ES_RESULT_SIZE);
    }

    public RyftProperties getMapping() {
        return ryftProperties.getRyftProperties(PropertiesProvider.RYFT_MAPPING);
    }

    protected RyftFormat getFormat() {
        return (RyftFormat) ryftProperties.get(PropertiesProvider.RYFT_FORMAT);
    }

    protected Boolean getCaseSensitive() {
        return ryftProperties.getBool(PropertiesProvider.RYFT_CASE_SENSITIVE);
    }

    public ObjectNode getParsedQuery() {
        return parsedQuery;
    }
}
