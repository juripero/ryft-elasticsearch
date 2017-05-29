package com.ryft.elasticsearch.converter;

public class ElasticConverterUnknown implements ElasticConvertingElement<Void> {

    final static String NAME = "unknown";

    @Override
    public Void convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        ElasticConversionUtil.getNextElasticPrimitive(convertingContext);
        return null;
    }

}
