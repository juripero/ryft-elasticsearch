package com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class RyftExpressionIPv4 extends RyftExpressionRange {

    public RyftExpressionIPv4(String valueA, RyftOperatorCompare operatorA, RyftOperatorCompare operatorB, String valueB) {
        super(String.format("\"%s\"", valueA), operatorA, operatorB, String.format("\"%s\"", valueB), "IPV4", "IP");
    }

    public RyftExpressionIPv4(String valueA, RyftOperatorCompare operatorA) {
        this(valueA, operatorA, null, null);
    }

    @Override
    public RyftExpression toLineExpression() {
        return new RyftExpressionIPv4(valueA, operatorA, operatorB.orElse(null), valueB.orElse(null), variableName, true);
    }

    private RyftExpressionIPv4(String valueA, RyftOperatorCompare operatorA, RyftOperatorCompare operatorB, String valueB, String variableName, boolean line) {
        super(valueA, operatorA, "IPV4", variableName);
        this.valueB = Optional.ofNullable(valueB);
        this.operatorB = Optional.ofNullable(operatorB);
        this.line = line;
    }
}
