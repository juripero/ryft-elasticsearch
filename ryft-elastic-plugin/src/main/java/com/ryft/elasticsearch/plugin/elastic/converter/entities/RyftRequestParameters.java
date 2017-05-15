package com.ryft.elasticsearch.plugin.elastic.converter.entities;

import com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl.RyftQuery;
import com.ryft.elasticsearch.plugin.elastic.plugin.RyftProperties;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;

public class RyftRequestParameters {

    private final RyftProperties ryftProperties;
    private final RyftQuery query;
    private final String[] indices;

    @Inject
    public RyftRequestParameters(RyftProperties ryftProperties,
            @Assisted RyftQuery ryftQuery, @Assisted String[] indices) {
        this.ryftProperties = new RyftProperties();
        this.ryftProperties.putAll(ryftProperties);
        this.query = ryftQuery;
        this.indices = indices;
    }

    public RyftQuery getQuery() {
        return query;
    }

    public String[] getIndices() {
        return indices;
    }

    public RyftProperties getRyftProperties() {
        return ryftProperties;
    }

    @Override
    public String toString() {
        return "RyftRequestParameters{" + "query=" + query + ", indices=" + indices + '}';
    }

}
