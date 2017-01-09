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
    private String searchString;
    private RyftFuzzyMetric metric;

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
        this.searchString = searchString;
        this.metric = metric;
        this.distance = distance;
    }

    @Override
    protected List<String> getParameters() {
        List<String> result = super.getParameters();
        result.add(String.format("DIST=%d", distance));
        return result;
    }

    @Override
    public RyftExpression toLineExpression() {
        return new RyftExpressionFuzzySearch(searchString, metric, distance, true);
    }

}
