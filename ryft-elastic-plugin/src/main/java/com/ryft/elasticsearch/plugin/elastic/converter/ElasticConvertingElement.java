package com.ryft.elasticsearch.plugin.elastic.converter;

import com.ryft.elasticsearch.plugin.utils.Try;

public interface ElasticConvertingElement<T> {

    Try<T> convert(ElasticConvertingContext convertingContext);

}
