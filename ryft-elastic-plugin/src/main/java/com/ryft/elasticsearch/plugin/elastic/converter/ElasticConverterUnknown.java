package com.ryft.elasticsearch.plugin.elastic.converter;

import com.ryft.elasticsearch.plugin.utils.Try;

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
