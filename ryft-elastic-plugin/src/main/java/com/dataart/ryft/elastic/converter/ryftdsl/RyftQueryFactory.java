package com.dataart.ryft.elastic.converter.ryftdsl;

import com.dataart.ryft.elastic.converter.ryftdsl.RyftExpressionFuzzySearch.RyftFuzzyMetric;
import java.util.List;

public class RyftQueryFactory {

    public RyftQuery buildFuzzyQuery(RyftFuzzyMetric metric, Integer fuzziness, String searchText, String fieldName) {
        if ((fieldName != null) && (searchText != null)
                && (fuzziness != null) && (metric != null)) {
            RyftExpression ryftExpression;
            if (fuzziness == 0) {
                ryftExpression = new RyftExpressionExactSearch(searchText);
            } else {
                ryftExpression = new RyftExpressionFuzzySearch(searchText, metric, fuzziness);
            }
            return new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName),
                    RyftOperator.CONTAINS, ryftExpression);
        } else {
            return null;
        }
    }
    
    public RyftQuery buildComplexQuery(RyftQueryComplex.RyftLogicalOperator operator, List<RyftQuery> operands) {
        return new RyftQueryComplex(operator, operands);
    }
}
