package com.ryft.elasticsearch.plugin.elastic.converter;

import static com.ryft.elasticsearch.plugin.elastic.plugin.PropertiesProvider.SEARCH_QUERY_SIZE;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class ElasticConverterSize implements ElasticConvertingElement<Void> {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterSize.class);
    final static String NAME = "size";
    
    @Override
    public Void convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
        Integer limit = ElasticConversionUtil.getInteger(convertingContext);
        convertingContext.getQueryProperties().put(SEARCH_QUERY_SIZE, limit);
        return null;
    }

}
