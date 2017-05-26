package com.ryft.elasticsearch.converter;

public interface ElasticConvertingElement<T> {

    T convert(ElasticConvertingContext convertingContext) throws ElasticConversionException;

}
