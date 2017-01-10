package com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl;

public class RyftExpressionExactSearch extends RyftExpression {

    private String searchString;

    private RyftExpressionExactSearch(String expressionName, String searchString) {
        super(expressionName, String.format("\"%s\"", searchString));
        this.searchString = searchString;
    }

    public RyftExpressionExactSearch(String searchString) {
        this("", searchString);
    }

    public RyftExpressionExactSearch(String searchString, Integer width) {
        this("ES", searchString);
        this.width = width;
    }

    public RyftExpressionExactSearch(String searchString, Boolean line) {
        this("ES", searchString);
        this.line = line;
    }

    @Override
    public RyftExpression toLineExpression() {
        return new RyftExpressionExactSearch(searchString, true);
    }
}
