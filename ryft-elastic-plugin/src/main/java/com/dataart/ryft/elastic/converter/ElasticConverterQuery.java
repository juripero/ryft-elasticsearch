package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import com.dataart.ryft.utils.Try;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class ElasticConverterQuery implements ElasticConvertingElement<RyftQuery> {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterQuery.class);

    static final String NAME = "query";

    @Override
    public Try<RyftQuery> convert(ElasticConvertingContext convertingContext) {
        LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
        return Try.apply(() -> {
            String currentName = ElasticConversionUtil.getNextElasticPrimitive(convertingContext);
            return convertingContext.getElasticConverter(currentName)
                    .map(converter -> (RyftQuery) converter.convert(convertingContext).getResultOrException())
                    .getResultOrException();
        });
    }
}
