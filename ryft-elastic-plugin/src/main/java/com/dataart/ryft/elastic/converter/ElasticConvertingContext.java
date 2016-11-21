package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.elastic.converter.ryftdsl.RyftOperator;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQueryFactory;
import com.dataart.ryft.utils.Try;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
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

    public static enum ElasticBoolSearchType {
        MUST, MUST_NOT, SHOULD
    }

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConvertingContext.class);

    private final XContentParser contentParser;
    private final Map<String, ElasticConvertingElement> elasticConverters;
    private final String originalQuery;
    private final RyftQueryFactory ryftQueryFactory;
    private ElasticSearchType searchType;
    private RyftOperator ryftOperator = RyftOperator.CONTAINS;
    private final Map<String, Object> queryProperties;
    private Integer minimumShouldMatch = 1;

    @Inject
    public ElasticConvertingContext(@Assisted XContentParser parser, @Assisted String originalQuery,
            Map<String, ElasticConvertingElement> injectedConverters,
            RyftQueryFactory ryftQueryFactory) {
        this.elasticConverters = ImmutableMap.copyOf(injectedConverters);
        this.originalQuery = originalQuery;
        this.contentParser = parser;
        this.ryftQueryFactory = ryftQueryFactory;
        this.queryProperties = new HashMap<>();
    }

    public XContentParser getContentParser() {
        return contentParser;
    }

    public Try<ElasticConvertingElement> getElasticConverter(String name) {
        return Try.apply(() -> {
            ElasticConvertingElement result = elasticConverters.get(name);
            if (result == null) {
                LOGGER.debug("Failed to find appropriate converter for token: '{}'", name);
                result = elasticConverters.get(ElasticConverterUnknown.NAME);
            }
            LOGGER.debug("Return converter {}", result.getClass().getSimpleName());
            return result;
        });
    }

    public String getOriginalQuery() {
        return originalQuery;
    }

    public RyftOperator getRyftOperator() {
        return ryftOperator;
    }

    public void setRyftOperator(RyftOperator ryftOperator) {
        this.ryftOperator = ryftOperator;
    }

    public ElasticSearchType getSearchType() {
        return searchType;
    }

    public void setSearchType(ElasticSearchType searchType) {
        this.searchType = searchType;
    }

    public Integer getMinimumShouldMatch() {
        return minimumShouldMatch;
    }

    public void setMinimumShouldMatch(Integer minimumShouldMatch) {
        if ((minimumShouldMatch != null) && (minimumShouldMatch >= 1)) {
            this.minimumShouldMatch = minimumShouldMatch;
        }
    }

    public Map<String, Object> getQueryProperties() {
        return queryProperties;
    }

    public RyftQueryFactory getQueryFactory() {
        return ryftQueryFactory;
    }

}
