package com.dataart.ryft.elastic.converter;

import org.elasticsearch.common.xcontent.XContentParser;

public interface ContextFactory {
    public ElasticConvertingContext create(XContentParser parser, String originalQuery);
}
