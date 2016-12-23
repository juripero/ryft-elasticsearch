package com.dataart.ryft.elastic.converter.ryftdsl;

public class RyftExpressionExactSearch extends RyftExpression {

    private RyftExpressionExactSearch(String expressionName, String searchString) {
        super(expressionName, String.format("\"%s\"", searchString));
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

}
