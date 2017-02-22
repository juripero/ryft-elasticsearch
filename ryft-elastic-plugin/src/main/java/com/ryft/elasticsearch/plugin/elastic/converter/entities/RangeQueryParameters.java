package com.ryft.elasticsearch.plugin.elastic.converter.entities;

import com.ryft.elasticsearch.plugin.elastic.converter.ElasticConversionException;
import com.ryft.elasticsearch.plugin.elastic.converter.ElasticConvertingContext;
import com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl.RyftExpressionRange;

import java.util.Map;

/**
 * Contains parsed query parameters that are required to create a ryft query, for cases where the
 * {@link ElasticConvertingContext.ElasticSearchType} is "range"
 * Used by {@link com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl.RyftQueryFactory}
 */
public class RangeQueryParameters extends QueryParameters<String> {

    private ElasticConvertingContext.ElasticDataType dataType;
    private String format = "yyyy-MM-dd HH:mm:ss";
    private Map<RyftExpressionRange.RyftOperatorCompare, String> lowerBound;
    private Map<RyftExpressionRange.RyftOperatorCompare, String> upperBound;
    private String separator = ",";
    private String decimal = ".";

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

    public Map<RyftExpressionRange.RyftOperatorCompare, String> getLowerBound() {
        return lowerBound;
    }

    public void setLowerBound(Map<RyftExpressionRange.RyftOperatorCompare, String> lowerBound) {
        this.lowerBound = lowerBound;
    }

    public Map<RyftExpressionRange.RyftOperatorCompare, String> getUpperBound() {
        return upperBound;
    }

    public void setUpperBound(Map<RyftExpressionRange.RyftOperatorCompare, String> upperBound) {
        this.upperBound = upperBound;
    }

    public String getSeparator() {
        return separator;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }

    public String getDecimal() {
        return decimal;
    }

    public void setDecimal(String decimal) {
        this.decimal = decimal;
    }

    @Override
    public void check() throws ElasticConversionException {
        if (dataType == null) {
            throw new ElasticConversionException("Data type should be defined.");
        }

        if (lowerBound == null && upperBound == null) {
            throw new ElasticConversionException("Range must have either upper bound, lower bound, or both");
        }
    }
}
