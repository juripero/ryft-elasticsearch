package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import com.dataart.ryft.utils.Try;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class ElasticConverterFuzzy implements ElasticConvertingElement {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterFuzzy.class);

    final static String NAME1 = "match_phrase";
    final static String NAME2 = "fuzzy";

    @Override
    public Try<RyftQuery> convert(ElasticConvertingContext convertingContext) {
        LOGGER.debug("Start fuzzy parsing");
        return Try.apply(() -> {
            getNextElasticPrimitive(convertingContext);
            convertingContext.setSearchType(ElasticConvertingContext.ElasticSearchType.MATCH);
            return convertingContext.getElasticConverter(ElasticConverterField.NAME)
                    .flatMap(converter -> converter.convert(convertingContext))
                    .getResultOrException();
        });
    }

}
