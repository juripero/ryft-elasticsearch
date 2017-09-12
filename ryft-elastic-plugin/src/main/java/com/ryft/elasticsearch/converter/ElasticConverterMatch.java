package com.ryft.elasticsearch.converter;

import com.ryft.elasticsearch.converter.ElasticConvertingContext.ElasticSearchType;
import com.ryft.elasticsearch.converter.ryftdsl.RyftQuery;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class ElasticConverterMatch implements ElasticConvertingElement<RyftQuery> {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterFuzzy.class);

    static final String NAME = "match";

    @Override
    public RyftQuery convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
        convertingContext.setSearchType(ElasticSearchType.MATCH);
        ElasticConvertingElement<RyftQuery> converter
                = convertingContext.getElasticConverter(ElasticConverterField.NAME);
        return ElasticConversionUtil.getObject(convertingContext, converter);
    }

}