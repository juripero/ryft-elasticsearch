package com.ryft.elasticsearch.converter.entities;

import com.ryft.elasticsearch.converter.ryftdsl.RyftFormat;
import com.ryft.elasticsearch.converter.ryftdsl.RyftQuery;
import com.ryft.elasticsearch.plugin.PropertiesProvider;
import com.ryft.elasticsearch.plugin.RyftProperties;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;

public class RyftRequestParameters {

    private final RyftProperties ryftProperties;
    private final RyftQuery query;
    private final String[] indices;
    private final Map<String, Object> parsedQuery;

    @Inject
    public RyftRequestParameters(RyftProperties ryftProperties,
            @Assisted RyftQuery ryftQuery, @Assisted String[] indices, @Assisted Map<String, Object> parsedQuery) {
        this.ryftProperties = new RyftProperties();
        this.ryftProperties.putAll(ryftProperties);
        this.query = ryftQuery;
        this.indices = indices;
        this.parsedQuery = parsedQuery;
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

    public Boolean isFileSearch() {
        if (ryftProperties.containsKey(PropertiesProvider.RYFT_FILES_TO_SEARCH)
                && (ryftProperties.get(PropertiesProvider.RYFT_FILES_TO_SEARCH) instanceof List)) {
            return true;
        } else return ryftProperties.containsKey(PropertiesProvider.RYFT_FORMAT)
                && (ryftProperties.get(PropertiesProvider.RYFT_FORMAT).equals(RyftFormat.RAW)
                || ryftProperties.get(PropertiesProvider.RYFT_FORMAT).equals(RyftFormat.UTF8));
    }

    public Map<String, Object> getParsedQuery() {
        return parsedQuery;
    }

    @Override
    public String toString() {
        return "RyftRequestParameters{" + "query=" + query + ", indices=" + Arrays.toString(indices) + '}';
    }

}
