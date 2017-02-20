package com.ryft.elasticsearch.plugin.elastic.converter.entities;

import com.ryft.elasticsearch.plugin.elastic.converter.ElasticConversionException;
import com.ryft.elasticsearch.plugin.elastic.converter.ElasticConvertingContext;
import com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl.RyftExpressionFuzzySearch.RyftFuzzyMetric;
import com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl.RyftQueryComplex;
import static com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl.RyftQueryFactory.FUZZYNESS_AUTO_VALUE;

/**
 * Contains parsed query parameters that are required to create a ryft query
 * Used by {@link com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl.RyftQueryFactory}
 */
public class FuzzyQueryParameters extends QueryParameters<String> {

    public static RyftFuzzyMetric METRIC_DEFAULT = RyftFuzzyMetric.FEDS;
    public static Integer FUZZINESS_DEFAULT = FUZZYNESS_AUTO_VALUE;
    
    protected RyftFuzzyMetric metric = METRIC_DEFAULT;
    protected Integer fuzziness = FUZZINESS_DEFAULT;
    protected RyftQueryComplex.RyftLogicalOperator operator = RyftQueryComplex.RyftLogicalOperator.OR;
    protected ElasticConvertingContext.ElasticSearchType searchType = ElasticConvertingContext.ElasticSearchType.MATCH_PHRASE;
    protected Integer width;
    protected Boolean line;

    public void setMetric(RyftFuzzyMetric metric) {
        this.metric = metric;
    }

    public void setFuzziness(Integer fuzziness) {
        this.fuzziness = fuzziness;
    }

    public void setOperator(RyftQueryComplex.RyftLogicalOperator operator) {
        this.operator = operator;
    }

    public void setSearchType(ElasticConvertingContext.ElasticSearchType searchType) {
        this.searchType = searchType;
    }
    public void setWidth(Integer width) {
        this.width = width;
    }

    public void setLine(Boolean line) {
        this.line = line;
    }

    public RyftFuzzyMetric getMetric() {
        return metric;
    }

    public Integer getFuzziness() {
        return fuzziness;
    }

    public RyftQueryComplex.RyftLogicalOperator getOperator() {
        return operator;
    }

    public ElasticConvertingContext.ElasticSearchType getSearchType() {
        return searchType;
    }

    public Integer getWidth() {
        return width;
    }

    public Boolean getLine() {
        return line;
    }

    @Override
    public void check() throws ElasticConversionException {
        super.check();
        if (searchType == null) {
            throw new ElasticConversionException("Search type should be defined.");
        }
        if ((operator == null) && (ElasticConvertingContext.ElasticSearchType.MATCH.equals(searchType))) {
            throw new ElasticConversionException("Logical operator for match search should be defined.");
        }
        if (fuzziness == null) {
            throw new ElasticConversionException("Fuzziness should be defined.");
        }
        if (metric == null) {
            throw new ElasticConversionException("Metric should be defined.");
        }
    }
}
