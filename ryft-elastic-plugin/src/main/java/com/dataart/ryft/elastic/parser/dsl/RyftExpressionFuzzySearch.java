package com.dataart.ryft.elastic.parser.dsl;

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
    private final Integer distance;
    private final Integer width;

    public RyftExpressionFuzzySearch(String searchString, RyftFuzzyMetric metric, Integer distance, Integer width) {
        this.searchString = searchString;
        this.metric = metric;
        this.distance = distance;
        this.width = width;
    }

    @Override
    public String buildRyftString() {
        return String.format("%s(\"%s\", DIST=%d, WIDTH=%d)",
                metric.buildRyftString(), searchString, distance, width);
    }

}
