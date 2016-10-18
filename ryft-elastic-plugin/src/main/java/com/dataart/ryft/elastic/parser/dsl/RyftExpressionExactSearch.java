package com.dataart.ryft.elastic.parser.dsl;

public class RyftExpressionExactSearch extends RyftExpression {

    private final String searchString;

    public RyftExpressionExactSearch(String searchString) {
        this.searchString = searchString;
    }

    @Override
    public String buildRyftString() {
        return "\"" + searchString + "\"";
    }
}
