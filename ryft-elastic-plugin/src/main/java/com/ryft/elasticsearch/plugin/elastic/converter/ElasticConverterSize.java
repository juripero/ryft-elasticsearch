package com.ryft.elasticsearch.plugin.elastic.converter;

import static com.ryft.elasticsearch.plugin.elastic.plugin.PropertiesProvider.SEARCH_QUERY_SIZE;
import com.ryft.elasticsearch.plugin.utils.Try;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class ElasticConverterSize implements ElasticConvertingElement<Void> {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterSize.class);
    final static String NAME = "size";

    @Override
    public Try<Void> convert(ElasticConvertingContext convertingContext) {
        LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
        return Try.apply(() -> {
            Integer limit = ElasticConversionUtil.getInteger(convertingContext);
            convertingContext.getQueryProperties().put(SEARCH_QUERY_SIZE, limit);
            return null;
        });
    }

}
