package com.ryft.elasticsearch.plugin.elastic.converter;

import com.ryft.elasticsearch.plugin.elastic.converter.ElasticConvertingContext.ElasticSearchType;
import com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl.RyftQuery;
import com.ryft.elasticsearch.plugin.utils.Try;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class ElasticConverterMatchPhrase<RyftQuery> implements ElasticConvertingElement<RyftQuery> {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterFuzzy.class);

    final static String NAME = "match_phrase";

    @Override
    public Try<RyftQuery> convert(ElasticConvertingContext convertingContext) {
        LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
        return Try.apply(() -> {
            convertingContext.setSearchType(ElasticSearchType.MATCH_PHRASE);
            ElasticConvertingElement<RyftQuery> converter = 
                    convertingContext.getElasticConverter(ElasticConverterField.NAME).getResultOrException();
            return ElasticConversionUtil.getObject(convertingContext, converter);
        });
    }

}
