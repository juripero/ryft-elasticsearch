package com.ryft.elasticsearch.converter.ryftdsl;

public class RyftExpressionRegex extends RyftExpression {

    public RyftExpressionRegex(String expression) {
        super("PCRE2");
        this.value = expression;
    }

    public RyftExpressionRegex(String expression, Integer width) {
        super("PCRE2", String.format("\"%s\"", expression), width);
        this.value = expression;
    }

    public RyftExpressionRegex(String expression, Boolean line) {
        super("PCRE2", String.format("\"%s\"", expression), line);
        this.value = expression;
    }
}
