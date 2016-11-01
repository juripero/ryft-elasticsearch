package com.dataart.ryft.elastic.converter;

import static com.dataart.ryft.elastic.plugin.PropertiesProvider.RYFT_INTEGRATION_ENABLED;
import com.dataart.ryft.utils.Try;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class ElasticConverterRyftEnabled implements ElasticConvertingElement<Boolean> {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterRyftEnabled.class);
    final static String NAME = RYFT_INTEGRATION_ENABLED;

    @Override
    public Try<Boolean> convert(ElasticConvertingContext convertingContext) {
        LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
        return Try.apply(() -> {
            Boolean isRyftIntegrationElabled = ElasticConversionUtil.getBoolean(convertingContext);
            convertingContext.getQueryProperties().put(RYFT_INTEGRATION_ENABLED, isRyftIntegrationElabled);
            return isRyftIntegrationElabled;
        });
    }
    
}
