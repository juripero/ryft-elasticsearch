package com.ryft.elasticsearch.converter.ryftdsl;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class RyftExpression implements RyftDslToken {

    protected final String expressionName;
    protected String value;

    protected Boolean line = null;
    protected Integer width = null;

    public RyftExpression(String expressionName) {
        this.expressionName = expressionName;
    }

    public RyftExpression(String expressionName, String value) {
        this(expressionName);
        this.value = value;
    }

    public RyftExpression(String expressionName, String value, Boolean line) {
        this(expressionName, value);
        this.line = line;
    }

    public RyftExpression(String expressionName, String value, Integer width) {
        this(expressionName, value);
        this.width = width;
    }

    protected List<String> getParameters() {
        List<String> result = new ArrayList<>();
        if (line != null) {
            result.add(String.format("LINE=%b", line));
        }
        if (width != null) {
            result.add(String.format("WIDTH=%d", width));
        }
        return result;
    }

    @Override
    public String buildRyftString() {
        String argument = Stream.concat(Stream.of(value), getParameters().stream()).collect(Collectors.joining(", "));
        if ((expressionName != null) && (!expressionName.isEmpty())) {
            return String.format("%s(%s)", expressionName, argument);
        } else {
            return value;
        }
    }

    public RyftExpression toLineExpression() {
        this.line = true;
        this.width = null;
        return this;
    }

    public RyftExpression toWidthExpression(Integer width) {
        this.width = width;
        return this;
    }
}