package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import com.dataart.ryft.utils.Try;

public interface ElasticConvertingElement {

    Try<RyftQuery> convert(ElasticConvertingContext convertingContext);

}