package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.utils.Try;

public class ElasticConverterUnknown implements ElasticConvertingElement<Void> {

    final static String NAME = "unknown";
    
    @Override
    public Try<Void> convert(ElasticConvertingContext convertingContext) {
        return Try.apply(() -> {
            ElasticConversionUtil.getNextElasticPrimitive(convertingContext);
            return null;
        });
    }

}
