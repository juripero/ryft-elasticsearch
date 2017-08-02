package com.ryft.elasticsearch.converter.entities;

import com.ryft.elasticsearch.converter.ElasticConverterRyft;
import com.ryft.elasticsearch.converter.ryftdsl.RyftQuery;
import com.ryft.elasticsearch.plugin.PropertiesProvider;
import com.ryft.elasticsearch.plugin.RyftProperties;
import java.util.Arrays;
import java.util.List;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;

public class RyftRequestParameters {

    private final RyftProperties ryftProperties;
    private final RyftQuery query;
    private final String[] indices;
    private final List<AbstractAggregationBuilder> aggregationBuilders;

    @Inject
    public RyftRequestParameters(RyftProperties ryftProperties,
            @Assisted RyftQuery ryftQuery, @Assisted String[] indices, @Assisted List<AbstractAggregationBuilder> aggregationBuilders) {
        this.ryftProperties = new RyftProperties();
        this.ryftProperties.putAll(ryftProperties);
        this.query = ryftQuery;
        this.indices = indices;
        this.aggregationBuilders = aggregationBuilders;
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

    public List<AbstractAggregationBuilder> getAggregations() {
        return aggregationBuilders;
    }

    @Override
    public String toString() {
        return "RyftRequestParameters{" + "query=" + query + ", indices=" + Arrays.toString(indices) + '}';
    }

}
