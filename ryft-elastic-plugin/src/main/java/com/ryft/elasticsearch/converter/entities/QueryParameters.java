package com.ryft.elasticsearch.converter.entities;

import com.ryft.elasticsearch.converter.ElasticConversionException;
import com.ryft.elasticsearch.converter.ryftdsl.RyftOperator;

public abstract class QueryParameters<T> {

    protected T searchValue = null;
    protected String fieldName = null;
    protected RyftOperator ryftOperator = RyftOperator.CONTAINS;

    public void setSearchValue(T searchValue) {
        this.searchValue = searchValue;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public void setRyftOperator(RyftOperator ryftOperator) {
        this.ryftOperator = ryftOperator;
    }

    public T getSearchValue() {
        return searchValue;
    }

    public String getFieldName() {
        return fieldName;
    }

    public RyftOperator getRyftOperator() {
        return ryftOperator;
    }

    public void check() throws ElasticConversionException {
        if (searchValue == null) {
            throw new ElasticConversionException("Search value should be defined.");
        }
        if (ryftOperator == null) {
            throw new ElasticConversionException("ryftOperator should be defined.");
        }
    }
}
