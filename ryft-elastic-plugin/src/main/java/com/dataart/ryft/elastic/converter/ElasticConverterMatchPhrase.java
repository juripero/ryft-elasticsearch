package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.elastic.converter.ElasticConvertingContext.ElasticSearchType;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import com.dataart.ryft.utils.Try;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class ElasticConverterMatchPhrase implements ElasticConvertingElement<RyftQuery> {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterFuzzy.class);

    final static String NAME = "match_phrase";

    @Override
    public Try<RyftQuery> convert(ElasticConvertingContext convertingContext) {
        LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
        return Try.apply(() -> {
            ElasticConversionUtil.getNextElasticPrimitive(convertingContext);
            convertingContext.setSearchType(ElasticSearchType.MATCH_PHRASE);
            return convertingContext.getElasticConverter(ElasticConverterField.NAME)
                    .flatMap(converter -> (Try<RyftQuery>) converter.convert(convertingContext))
                    .getResultOrException();
        });
    }

}
