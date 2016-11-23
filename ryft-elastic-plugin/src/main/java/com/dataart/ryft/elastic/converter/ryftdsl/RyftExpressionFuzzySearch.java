package com.dataart.ryft.elastic.converter.ryftdsl;

public class RyftExpressionFuzzySearch extends RyftExpression {

    public static enum RyftFuzzyMetric implements RyftDslToken {
        FEDS, FHS;

        @Override
        public String buildRyftString() {
            return name();
        }
    }

    private final String searchString;
    private final RyftFuzzyMetric metric;
    private Integer distance = null;
    private Integer width = null;

    public RyftExpressionFuzzySearch(String searchString, RyftFuzzyMetric metric, Integer distance, Integer width) {
        this.searchString = searchString;
        this.metric = metric;
        this.distance = distance;
        this.width = width;
    }

    public RyftExpressionFuzzySearch(String searchString, RyftFuzzyMetric metric, Integer distance) {
        this(searchString, metric, distance, null);
    }

    @Override
    public String buildRyftString() {
        StringBuilder result = new StringBuilder(metric.buildRyftString());
        result.append("(\"").append(searchString).append("\", DIST=").append(distance);
        if (width != null) {
            result.append(", WIDTH=").append(width);
        }
        result.append(")");
        return result.toString();
    }

    @Override
    public String toString() {
        return "RyftExpressionFuzzySearch{" + buildRyftString() + '}';
    }

}
