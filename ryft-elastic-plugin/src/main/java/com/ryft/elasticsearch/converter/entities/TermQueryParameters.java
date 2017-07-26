package com.ryft.elasticsearch.converter.entities;

import com.ryft.elasticsearch.converter.ElasticConvertingContext;

import java.util.List;

/**
 * Contains parsed query parameters that are required to create a ryft query, for cases where the
 * {@link ElasticConvertingContext.ElasticSearchType} is "term"
 * Used by {@link com.ryft.elasticsearch.converter.ryftdsl.RyftQueryFactory}
 */
public class TermQueryParameters extends QueryParameters<String> {

    private ElasticConvertingContext.ElasticDataType dataType = ElasticConvertingContext.ElasticDataType.STRING;
    private List<String> searchArray;
    private String format = "yyyy-MM-dd HH:mm:ss";
    private String separator = ",";
    private String decimal = ".";
    private String currency = "$";
    protected Integer width;
    protected Boolean line;

    public ElasticConvertingContext.ElasticDataType getDataType() {
        return dataType;
    }

    public void setDataType(ElasticConvertingContext.ElasticDataType dataType) {
        this.dataType = dataType;
    }

    public String getFormat() {
        return format;
    }

    public List<String> getSearchArray() {
        return searchArray;
    }

    public void setSearchArray(List<String> searchArray) {
        this.searchArray = searchArray;
    }

    public void setFormat(String format) {
        this.format = format;
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

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public Integer getWidth() {
        return width;
    }

    public void setWidth(Integer width) {
        this.width = width;
    }

    public Boolean getLine() {
        return line;
    }

    public void setLine(Boolean line) {
        this.line = line;
    }
}
