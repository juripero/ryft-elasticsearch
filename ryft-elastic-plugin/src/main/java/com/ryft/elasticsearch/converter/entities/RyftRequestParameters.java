package com.ryft.elasticsearch.converter.entities;

import com.ryft.elasticsearch.converter.ElasticConverterRyft;
import com.ryft.elasticsearch.converter.ryftdsl.RyftQuery;
import com.ryft.elasticsearch.plugin.PropertiesProvider;
import com.ryft.elasticsearch.plugin.RyftProperties;
import java.util.Arrays;
import java.util.List;
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

    public Boolean isFileSearch() {
        if (ryftProperties.containsKey(PropertiesProvider.RYFT_FILES_TO_SEARCH)
                && (ryftProperties.get(PropertiesProvider.RYFT_FILES_TO_SEARCH) instanceof List)) {
            return true;
        } else return ryftProperties.containsKey(PropertiesProvider.RYFT_FORMAT)
                && (ryftProperties.get(PropertiesProvider.RYFT_FORMAT).equals(ElasticConverterRyft.ElasticConverterFormat.RyftFormat.RAW)
                || ryftProperties.get(PropertiesProvider.RYFT_FORMAT).equals(ElasticConverterRyft.ElasticConverterFormat.RyftFormat.UTF8));
    }

    @Override
    public String toString() {
        return "RyftRequestParameters{" + "query=" + query + ", indices=" + Arrays.toString(indices) + '}';
    }

}
