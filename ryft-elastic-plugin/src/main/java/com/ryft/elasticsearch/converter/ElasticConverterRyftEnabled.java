package com.ryft.elasticsearch.converter;

import static com.ryft.elasticsearch.plugin.PropertiesProvider.RYFT_INTEGRATION_ENABLED;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class ElasticConverterRyftEnabled implements ElasticConvertingElement<Void> {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterRyftEnabled.class);
    final static String NAME = "ryft_enabled";
    
    @Override
    public Void convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
        Boolean isRyftIntegrationElabled = ElasticConversionUtil.getBoolean(convertingContext);
        convertingContext.getQueryProperties().put(RYFT_INTEGRATION_ENABLED, isRyftIntegrationElabled);
        return null;
    }
    
}
