package com.dataart.ryft.elastic.converter.ryftdsl;

import java.util.List;

public class RyftExpressionFuzzySearch extends RyftExpression {

    public static enum RyftFuzzyMetric implements RyftDslToken {
        FEDS, FHS;

        @Override
        public String buildRyftString() {
            return name();
        }
    }

    private Integer distance = null;

    public RyftExpressionFuzzySearch(String searchString, RyftFuzzyMetric metric, Integer distance, Boolean line) {
        this(searchString, metric, distance);
        this.line = line;
    }

    public RyftExpressionFuzzySearch(String searchString, RyftFuzzyMetric metric, Integer distance, Integer width) {
        this(searchString, metric, distance);
        this.width = width;
    }

    public RyftExpressionFuzzySearch(String searchString, RyftFuzzyMetric metric, Integer distance) {
        super(metric.buildRyftString(), String.format("\"%s\"", searchString));
        this.distance = distance;
    }

    @Override
    protected List<String> getParameters() {
        List<String> result = super.getParameters();
        result.add(String.format("DIST=%d", distance));
        return result;
    }

}
