package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;

public interface ElasticConvertingElement {

    RyftQuery convert(ElasticConvertingContext convertingContext) throws ElasticConversionException;
}
