package com.ryft.elasticsearch.plugin.elastic.converter;

public interface ElasticConvertingElement<T> {

    T convert(ElasticConvertingContext convertingContext) throws ElasticConversionException;

}
