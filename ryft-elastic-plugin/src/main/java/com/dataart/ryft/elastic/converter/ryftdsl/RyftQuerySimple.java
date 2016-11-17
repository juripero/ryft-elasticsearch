package com.dataart.ryft.elastic.converter.ryftdsl;

public class RyftQuerySimple implements RyftQuery {

    private final RyftInputSpecifier inputSpecifier;
    private final RyftOperator operator;
    private final RyftExpression expression;

    public RyftQuerySimple(RyftInputSpecifier inputSpecifier, RyftOperator operator, RyftExpression expression) {
        this.inputSpecifier = inputSpecifier;
        this.operator = operator;
        this.expression = expression;
    }

    @Override
    public String buildRyftString(Boolean isIndexedSearch) {
        return String.format("(%s %s %s)", inputSpecifier.buildRyftString(isIndexedSearch),
                operator.buildRyftString(isIndexedSearch), expression.buildRyftString(isIndexedSearch));
    }

    @Override
    public String toString() {
        return "RyftQuerySimple{" + buildRyftString(false) + '}';
    }

}
