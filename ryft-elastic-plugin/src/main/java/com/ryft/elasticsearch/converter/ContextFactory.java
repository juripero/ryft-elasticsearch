package com.ryft.elasticsearch.converter;

import org.elasticsearch.action.search.SearchRequest;

public interface ContextFactory {

    ElasticConvertingContext create(SearchRequest searchRequest);
}
