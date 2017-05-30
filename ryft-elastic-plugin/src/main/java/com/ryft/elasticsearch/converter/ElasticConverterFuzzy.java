package com.ryft.elasticsearch.converter;

import com.ryft.elasticsearch.converter.ElasticConvertingContext.ElasticSearchType;
import com.ryft.elasticsearch.converter.ryftdsl.RyftQuery;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class ElasticConverterFuzzy implements ElasticConvertingElement {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterFuzzy.class);

    final static String NAME = "fuzzy";

    @Override
    public Object convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
        convertingContext.setSearchType(ElasticSearchType.FUZZY);
        ElasticConvertingElement<RyftQuery> converter
                = convertingContext.getElasticConverter(ElasticConverterField.NAME);
        return ElasticConversionUtil.getObject(convertingContext, converter);
    }

}
