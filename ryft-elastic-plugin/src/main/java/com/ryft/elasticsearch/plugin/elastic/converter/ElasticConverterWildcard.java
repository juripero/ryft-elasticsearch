package com.ryft.elasticsearch.plugin.elastic.converter;

import com.ryft.elasticsearch.plugin.utils.Try;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class ElasticConverterWildcard<RyftQuery> implements ElasticConvertingElement {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterWildcard.class);

    final static String NAME = "wildcard";

    @Override
    public Try<RyftQuery> convert(ElasticConvertingContext convertingContext) {
        LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
        return Try.apply(() -> {
            convertingContext.setSearchType(ElasticConvertingContext.ElasticSearchType.WILDCARD);
            ElasticConvertingElement<RyftQuery> converter =
                    convertingContext.getElasticConverter(ElasticConverterField.NAME).getResultOrException();
            return ElasticConversionUtil.getObject(convertingContext, converter);
        });
    }
}
