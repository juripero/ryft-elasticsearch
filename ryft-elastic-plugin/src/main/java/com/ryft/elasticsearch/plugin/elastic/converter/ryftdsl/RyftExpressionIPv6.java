package com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl;

public class RyftExpressionIPv6 extends RyftExpressionRange {

    public RyftExpressionIPv6(String valueA, RyftOperatorCompare operatorA, RyftOperatorCompare operatorB, String valueB) {
        super(String.format("\"%s\"", valueA), operatorA, operatorB, String.format("\"%s\"", valueB), "IPV6", "IP");
    }

    public RyftExpressionIPv6(String valueA, RyftOperatorCompare operatorA) {
        this(valueA, operatorA, null, null);
    }

}
