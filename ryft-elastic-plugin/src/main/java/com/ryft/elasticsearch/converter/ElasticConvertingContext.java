package com.ryft.elasticsearch.converter;

import com.ryft.elasticsearch.converter.ryftdsl.RyftOperator;
import com.ryft.elasticsearch.converter.ryftdsl.RyftQueryFactory;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

public class ElasticConvertingContext {

    public static enum ElasticSearchType {
        MATCH, MATCH_PHRASE, FUZZY, WILDCARD, TERM, RANGE
    }

    public static enum ElasticDataType {
        STRING, DATETIME, NUMBER, NUMBER_ARRAY, CURRENCY, IPV4, IPV6
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
    private Boolean minimumShouldMatchDefined = false;
    private Boolean line;
    private Integer width;
    private ElasticDataType dataType = ElasticDataType.STRING;
    private List<String> searchArray; //FIXME - workaround for timeseries
    private String[] indices;
    private String agg = "";

    @Inject
    public ElasticConvertingContext(@Assisted SearchRequest searchRequest,
            Map<String, ElasticConvertingElement> injectedConverters,
            RyftQueryFactory ryftQueryFactory) throws IOException {
        this.elasticConverters = ImmutableMap.copyOf(injectedConverters);
        BytesReference searchContent = searchRequest.source();
        String queryString = (searchContent == null) ? "" : searchContent.toUtf8();
        this.originalQuery = queryString;
        this.contentParser = XContentFactory.xContent(queryString).createParser(queryString);
        this.indices = searchRequest.indices();
        this.ryftQueryFactory = ryftQueryFactory;
        this.queryProperties = new HashMap<>();
    }

    public XContentParser getContentParser() {
        return contentParser;
    }

    public ElasticConvertingElement getElasticConverter(String name) {
        ElasticConvertingElement result = elasticConverters.get(name);
        if (result == null) {
            LOGGER.debug("Failed to find appropriate converter for token: '{}'", name);
            result = elasticConverters.get(ElasticConverterUnknown.NAME);
        }
        LOGGER.debug("Return converter {}", result.getClass().getSimpleName());
        return result;
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
            minimumShouldMatchDefined = true;
            this.minimumShouldMatch = minimumShouldMatch;
        }
    }

    public Boolean isMinimumShouldMatchDefined() {
        return minimumShouldMatchDefined;
    }

    public Boolean getLine() {
        return line;
    }

    public void setLine(Boolean line) {
        this.line = line;
    }

    public Integer getWidth() {
        return width;
    }

    public void setWidth(Integer width) {
        this.width = width;
    }

    public ElasticDataType getDataType() {
        return dataType;
    }

    public void setDataType(ElasticDataType dataType) {
        this.dataType = dataType;
    }

    public Map<String, Object> getQueryProperties() {
        return queryProperties;
    }

    public RyftQueryFactory getQueryFactory() {
        return ryftQueryFactory;
    }

    public List<String> getSearchArray() {
        return searchArray;
    }

    public void setSearchArray(List<String> searchArray) {
        this.searchArray = searchArray;
    }

    public String[] getIndices() {
        return indices;
    }

    public void setIndices(String[] indices) {
        this.indices = indices;
    }

    public String getAgg() {
        return agg;
    }

    public void setAgg(String agg) {
        this.agg = agg;
    }
}
