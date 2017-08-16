package com.ryft.elasticsearch.plugin.disruptor.messages;

import com.ryft.elasticsearch.rest.client.RyftSearchException;
import com.ryft.elasticsearch.converter.ryftdsl.RyftFormat;
import com.ryft.elasticsearch.converter.ryftdsl.RyftQuery;
import com.ryft.elasticsearch.plugin.PropertiesProvider;
import com.ryft.elasticsearch.plugin.RyftProperties;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;

public abstract class SearchRequestEvent extends RequestEvent {

    protected final ClusterState clusterState;

    protected final RyftProperties ryftProperties;

    protected final String query;
    protected final String encodedQuery;

    protected final Map<String, Object> parsedQuery;

    @Inject
    protected SearchRequestEvent(ClusterService clusterService,
                                 @Assisted RyftProperties ryftProperties,
                                 @Assisted RyftQuery query,
                                 @Assisted Map<String, Object> parsedQuery) throws RyftSearchException {
        super();
        this.clusterState = clusterService.state();
        this.ryftProperties = new RyftProperties();
        this.ryftProperties.putAll(ryftProperties);
        this.query = query.buildRyftString();
        this.parsedQuery = parsedQuery;
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
        return ryftProperties.getInt(PropertiesProvider.SEARCH_QUERY_LIMIT);
    }

    protected RyftFormat getFormat() {
        return (RyftFormat) ryftProperties.get(PropertiesProvider.RYFT_FORMAT);
    }

    protected Boolean getCaseSensitive() {
        return ryftProperties.getBool(PropertiesProvider.RYFT_CASE_SENSITIVE);
    }

    public Map<String, Object> getParsedQuery() {
        return parsedQuery;
    }
}
