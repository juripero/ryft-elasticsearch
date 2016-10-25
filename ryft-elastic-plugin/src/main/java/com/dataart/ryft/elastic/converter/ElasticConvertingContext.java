package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.elastic.converter.ryftdsl.RyftExpression;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftExpressionExactSearch;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftExpressionFuzzySearch;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftExpressionFuzzySearch.RyftFuzzyMetric;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftInputSpecifierRecord;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftOperator;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuerySimple;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Set;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.xcontent.XContentParser;

public class ElasticConvertingContext {

    private final XContentParser contentParser;
    private final Map<String, ElasticConvertingElement> elasticConverters;

    // TODO: [dendec] add default values
    private RyftFuzzyMetric metric = null;
    private Integer fuzziness = null;
    private String searchText = null;
    private String fieldName = null;
    private String originalQuery = null;

    @Inject
    public ElasticConvertingContext(@Assisted XContentParser parser, Set<ElasticConvertingElement> injectedConverters) {
        Map<String, ElasticConvertingElement> convertersMap = Maps.newHashMap();
        for (ElasticConvertingElement converter : injectedConverters) {
            for (String name : converter.names()) {
                convertersMap.put(name, converter);
            }
        }
        this.elasticConverters = ImmutableMap.copyOf(convertersMap);
        this.contentParser = parser;
    }

    public XContentParser getContentParser() {
        return contentParser;
    }

    public ElasticConvertingElement getElasticConverter(String name) throws ClassNotFoundException {
        ElasticConvertingElement result = elasticConverters.get(name);
        if (result == null) {
            throw new ClassNotFoundException(String.format("Can not find elastic parser for: %s", name));
        }
        return result;
    }

    public RyftFuzzyMetric getMetric() {
        return metric;
    }

    public void setMetric(RyftFuzzyMetric metric) {
        this.metric = metric;
    }

    public Integer getFuzziness() {
        return fuzziness;
    }

    public void setFuzziness(Integer fuzziness) {
        this.fuzziness = fuzziness;
    }

    public String getSearchText() {
        return searchText;
    }

    public void setSearchText(String searchText) {
        this.searchText = searchText;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public void setOriginalQuery(String originalQuery) {
        this.originalQuery = originalQuery;
    }

    public String getOriginalQuery() {
        return originalQuery;
    }

    public void clean() {
        fieldName = null;
        searchText = null;
        fuzziness = null;
        metric = null;
    }

    RyftQuery constructFuzzyQuery() {
        if ((fieldName != null) && (searchText != null) && (fuzziness != null) && (metric != null)) {
            RyftExpression ryftExpression;
            if (fuzziness == 0) {
                ryftExpression = new RyftExpressionExactSearch(searchText);
            } else {
                ryftExpression = new RyftExpressionFuzzySearch(searchText, metric, fuzziness);
            }
            return new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName), RyftOperator.CONTAINS, ryftExpression);
        } else {
            return null;
        }
    }

}
