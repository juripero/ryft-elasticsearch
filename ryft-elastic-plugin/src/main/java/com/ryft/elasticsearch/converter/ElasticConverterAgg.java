package com.ryft.elasticsearch.converter;

import com.ryft.elasticsearch.converter.ryftdsl.RyftQuery;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class ElasticConverterAgg implements ElasticConvertingElement<RyftQuery> {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterAgg.class);

    static final String NAME = "aggs";

    @Override
    public RyftQuery convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        LOGGER.info(String.format("!!Start \"%s\" parsing", NAME));
        String currentName = ElasticConversionUtil.getNextElasticPrimitive(convertingContext);
        convertingContext.setAgg("Agg!");
        return (RyftQuery) convertingContext.getElasticConverter(currentName).convert(convertingContext);
    }
}
