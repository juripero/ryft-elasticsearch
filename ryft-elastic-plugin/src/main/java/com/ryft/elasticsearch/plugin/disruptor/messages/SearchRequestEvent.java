package com.ryft.elasticsearch.plugin.disruptor.messages;

import com.ryft.elasticsearch.converter.ElasticConversionCriticalException;
import com.ryft.elasticsearch.converter.ElasticConverterRyft;
import com.ryft.elasticsearch.converter.ryftdsl.RyftQuery;
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

    protected final String agg;

    @Inject
    protected SearchRequestEvent(ClusterService clusterService,
                                 @Assisted RyftProperties ryftProperties,
                                 @Assisted RyftQuery query,
                                 @Assisted String agg) throws ElasticConversionCriticalException {
        super();
        this.clusterState = clusterService.state();
        this.ryftProperties = new RyftProperties();
        this.ryftProperties.putAll(ryftProperties);
        this.agg = agg;
        try {
            this.query = URLEncoder.encode(query.buildRyftString(), "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            throw new ElasticConversionCriticalException(ex);
        }
    }

    protected void validateRequest() throws ElasticConversionCriticalException {
        if (ryftProperties.containsKey(PropertiesProvider.RYFT_FORMAT)
                && ryftProperties.get(PropertiesProvider.RYFT_FORMAT).equals(ElasticConverterRyft.ElasticConverterFormat.RyftFormat.UNKNOWN_FORMAT)) {
            throw new ElasticConversionCriticalException("Unknown format. Please use one of the following formats: json, xml, utf8, raw");
        }
    }

    protected Integer getLimit() {
        return ryftProperties.getInt(PropertiesProvider.SEARCH_QUERY_LIMIT);
    }

    protected ElasticConverterRyft.ElasticConverterFormat.RyftFormat getFormat() {
        return (ElasticConverterRyft.ElasticConverterFormat.RyftFormat) ryftProperties.get(PropertiesProvider.RYFT_FORMAT);
    }

    protected Boolean getCaseSensitive() {
        return ryftProperties.getBool(PropertiesProvider.RYFT_CASE_SENSITIVE);
    }

    public String getAgg() {
        return agg;
    }
}
