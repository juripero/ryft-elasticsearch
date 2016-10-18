package com.dataart.ryft.elastic.parser.dsl;

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
    public String buildRyftString() {
        return String.format("(%s %s %s)", inputSpecifier.buildRyftString(),
                operator.buildRyftString(), expression.buildRyftString());
    }

}
