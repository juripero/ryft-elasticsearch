package com.ryft.elasticsearch.plugin.elastic.converter;

import org.elasticsearch.action.search.SearchRequest;

public interface ContextFactory {

    ElasticConvertingContext create(SearchRequest searchRequest);
}
