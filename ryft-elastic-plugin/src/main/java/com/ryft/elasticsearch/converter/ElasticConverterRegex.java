package com.ryft.elasticsearch.converter;

import com.ryft.elasticsearch.converter.ryftdsl.RyftQuery;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class ElasticConverterRegex implements ElasticConvertingElement<RyftQuery> {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterRegex.class);

    final static String NAME = "regexp";

    @Override
    public RyftQuery convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
        convertingContext.setSearchType(ElasticConvertingContext.ElasticSearchType.REGEX);
        ElasticConvertingElement<RyftQuery> converter =
                convertingContext.getElasticConverter(ElasticConverterField.NAME);
        return (RyftQuery) ElasticConversionUtil.getObject(convertingContext, converter);
    }
}
