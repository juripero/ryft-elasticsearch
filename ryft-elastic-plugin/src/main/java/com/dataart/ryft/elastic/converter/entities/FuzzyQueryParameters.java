package com.dataart.ryft.elastic.converter.entities;

import com.dataart.ryft.elastic.converter.ElasticConversionException;
import com.dataart.ryft.elastic.converter.ElasticConvertingContext;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftExpressionFuzzySearch;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQueryComplex;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftExpressionFuzzySearch.RyftFuzzyMetric;

import static com.dataart.ryft.elastic.converter.ryftdsl.RyftQueryFactory.FUZZYNESS_AUTO_VALUE;

public class FuzzyQueryParameters extends QueryParameters<String> {

    protected RyftExpressionFuzzySearch.RyftFuzzyMetric metric = RyftFuzzyMetric.FEDS;
    protected Integer fuzziness = FUZZYNESS_AUTO_VALUE;
    protected RyftQueryComplex.RyftLogicalOperator operator = RyftQueryComplex.RyftLogicalOperator.OR;
    protected ElasticConvertingContext.ElasticSearchType searchType = ElasticConvertingContext.ElasticSearchType.MATCH_PHRASE;

    public void setMetric(RyftExpressionFuzzySearch.RyftFuzzyMetric metric) {
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

    public RyftExpressionFuzzySearch.RyftFuzzyMetric getMetric() {
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
