package com.dataart.ryft.elastic.converter;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentParser;

public class ElasticConvertingContext {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConvertingContext.class);

    private final XContentParser contentParser;
    private final Map<String, ElasticConvertingElement> elasticConverters;
    private final String originalQuery;

    @Inject
    public ElasticConvertingContext(@Assisted XContentParser parser, @Assisted String originalQuery,
            Map<String, ElasticConvertingElement> injectedConverters) {
        this.elasticConverters = ImmutableMap.copyOf(injectedConverters);
        this.originalQuery = originalQuery;
        this.contentParser = parser;
    }

    public XContentParser getContentParser() {
        return contentParser;
    }

    public ElasticConvertingElement getElasticConverter(String name) {
        ElasticConvertingElement result = elasticConverters.get(name);
        if (result == null) {
            LOGGER.warn("Failed to find appropriate converter for token: '{}' available converters {}.\n"
                    + " Original query: {}", name, elasticConverters.keySet(), originalQuery);
        } else {
            LOGGER.debug("Return converter {}", result);
        }
        return result;
    }

}
