package com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl;

public class RyftExpressionExactSearch extends RyftExpression {
    
    private final String searchString;

    public RyftExpressionExactSearch(String searchString) {
        super("", String.format("\"%s\"", searchString));
        this.searchString = searchString;
    }

    public RyftExpressionExactSearch(String searchString, Integer width) {
        super("ES", String.format("\"%s\"", searchString), width);
        this.searchString = searchString;
    }

    public RyftExpressionExactSearch(String searchString, Boolean line) {
        super("ES", String.format("\"%s\"", searchString), line);
        this.searchString = searchString;
    }

    @Override
    public RyftExpression toLineExpression() {
        return new RyftExpressionExactSearch(searchString, true);
    }

}
