package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.utils.Try;

public interface ElasticConvertingElement<T> {

    Try<T> convert(ElasticConvertingContext convertingContext);

}
