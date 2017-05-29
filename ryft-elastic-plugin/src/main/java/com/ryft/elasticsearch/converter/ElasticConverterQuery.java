package com.ryft.elasticsearch.converter;

import com.ryft.elasticsearch.converter.ryftdsl.RyftQuery;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class ElasticConverterQuery implements ElasticConvertingElement<RyftQuery> {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterQuery.class);

    static final String NAME = "query";

    @Override
    public RyftQuery convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
        String currentName = ElasticConversionUtil.getNextElasticPrimitive(convertingContext);
        return (RyftQuery) convertingContext.getElasticConverter(currentName).convert(convertingContext);
    }
}
