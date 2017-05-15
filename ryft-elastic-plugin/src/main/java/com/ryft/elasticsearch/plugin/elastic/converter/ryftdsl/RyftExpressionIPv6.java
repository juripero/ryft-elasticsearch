package com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl;

import java.util.Optional;

public class RyftExpressionIPv6 extends RyftExpressionRange {

    public RyftExpressionIPv6(String valueA, RyftOperatorCompare operatorA, RyftOperatorCompare operatorB, String valueB) {
        super(String.format("\"%s\"", valueA), operatorA, operatorB, String.format("\"%s\"", valueB), "IPV6", "IP");
    }

    public RyftExpressionIPv6(String valueA, RyftOperatorCompare operatorA) {
        this(valueA, operatorA, null, null);
    }

    @Override
    public RyftExpression toLineExpression() {
        return new RyftExpressionIPv6(valueA, operatorA, operatorB.orElse(null), valueB.orElse(null), variableName, true);
    }

    private RyftExpressionIPv6(String valueA, RyftOperatorCompare operatorA, RyftOperatorCompare operatorB, String valueB, String variableName, boolean line) {
        super(valueA, operatorA, "IPV6", variableName);
        this.valueB = Optional.ofNullable(valueB);
        this.operatorB = Optional.ofNullable(operatorB);
        this.line = line;
    }

}
