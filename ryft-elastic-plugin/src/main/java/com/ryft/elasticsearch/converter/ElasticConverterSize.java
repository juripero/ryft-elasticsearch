package com.ryft.elasticsearch.converter;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import static com.ryft.elasticsearch.plugin.PropertiesProvider.SEARCH_QUERY_LIMIT;

public class ElasticConverterSize implements ElasticConvertingElement<Void> {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterSize.class);
    final static String NAME = "size";
    
    @Override
    public Void convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
        Integer limit = (Integer)ElasticConversionUtil.getNumber(convertingContext);
        convertingContext.getQueryProperties().put(SEARCH_QUERY_LIMIT, limit);
        return null;
    }

}
