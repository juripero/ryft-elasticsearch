package com.dataart.ryft.elastic.converter.entities;

import com.dataart.ryft.elastic.converter.ElasticConversionException;
import com.dataart.ryft.elastic.converter.ElasticConvertingContext;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftExpressionFuzzySearch.RyftFuzzyMetric;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQueryComplex;
import static com.dataart.ryft.elastic.converter.ryftdsl.RyftQueryFactory.FUZZYNESS_AUTO_VALUE;

public class FuzzyQueryParameters extends QueryParameters<String> {

    public static RyftFuzzyMetric METRIC_DEFAULT = RyftFuzzyMetric.FEDS;
    public static Integer FUZZINESS_DEFAULT = FUZZYNESS_AUTO_VALUE;
    
    protected RyftFuzzyMetric metric = METRIC_DEFAULT;
    protected Integer fuzziness = FUZZINESS_DEFAULT;
    protected RyftQueryComplex.RyftLogicalOperator operator = RyftQueryComplex.RyftLogicalOperator.OR;
    protected ElasticConvertingContext.ElasticSearchType searchType = ElasticConvertingContext.ElasticSearchType.MATCH_PHRASE;

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
