package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.utils.Try;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentParser;

public class ElasticConvertingContext {

    public static enum ElasticSearchType {
        MATCH, MATCH_PHRASE, FUZZY
    }

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConvertingContext.class);

    private final XContentParser contentParser;
    private final Map<String, ElasticConvertingElement> elasticConverters;
    private final String originalQuery;
    private ElasticSearchType searchType;

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

    public Try<ElasticConvertingElement> getElasticConverter(String name) {
        return Try.apply(() -> {
            ElasticConvertingElement result = elasticConverters.get(name);
            if (result == null) {
                LOGGER.warn("Failed to find appropriate converter for token: '{}' available converters {}.\n"
                        + " Original query: {}", name, elasticConverters.keySet(), originalQuery);
                throw new ElasticConversionException("Failed to find appropriate converter for token: " + name);
            } else {
                LOGGER.debug("Return converter {}", result);
            }
            return result;
        });
    }

    public String getOriginalQuery() {
        return originalQuery;
    }

    public ElasticSearchType getSearchType() {
        return searchType;
    }

    public void setSearchType(ElasticSearchType searchType) {
        this.searchType = searchType;
    }

}
