package com.ryft.elasticsearch.plugin.elastic.converter.entities;

import com.ryft.elasticsearch.plugin.elastic.converter.ElasticConvertingContext;

/**
 * Contains parsed query parameters that are required to create a ryft query, for cases where the
 * {@link ElasticConvertingContext.ElasticSearchType} is "term"
 * Used by {@link com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl.RyftQueryFactory}
 */
public class TermQueryParameters extends QueryParameters<String> {

    private ElasticConvertingContext.ElasticDataType dataType;
    private String format;

    public ElasticConvertingContext.ElasticDataType getDataType() {
        return dataType;
    }

    public void setDataType(ElasticConvertingContext.ElasticDataType dataType) {
        this.dataType = dataType;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }
}
