package com.dataart.ryft.elastic.converter;

import static com.dataart.ryft.elastic.plugin.PropertiesProvider.SEARCH_QUERY_SIZE;
import com.dataart.ryft.utils.Try;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class ElasticConverterRyftLimit implements ElasticConvertingElement<Integer> {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterRyftEnabled.class);
    final static String NAME = SEARCH_QUERY_SIZE;

    @Override
    public Try<Integer> convert(ElasticConvertingContext convertingContext) {
        LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
        return Try.apply(() -> {
            Integer limit = ElasticConversionUtil.getInteger(convertingContext);
            convertingContext.getQueryProperties().put(SEARCH_QUERY_SIZE, limit);
            return limit;
        });
    }

}
