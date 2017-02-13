package com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl;

import java.util.ArrayList;
import java.util.List;

public class RyftExpressionRegex extends RyftExpression {

    public RyftExpressionRegex(String expression) {
        super("REGEX");
        value = expression;
    }

    @Override
    protected List<String> getParameters() {
        return new ArrayList<>();
    }

    @Override
    public RyftExpression toLineExpression() {
        //TODO:
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
