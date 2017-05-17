package com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl;

public class RyftExpressionIPv4 extends RyftExpressionRange {

    public RyftExpressionIPv4(String valueA, RyftOperatorCompare operatorA, RyftOperatorCompare operatorB, String valueB) {
        super(String.format("\"%s\"", valueA), operatorA, operatorB, String.format("\"%s\"", valueB), "IPV4", "IP");
    }

    public RyftExpressionIPv4(String valueA, RyftOperatorCompare operatorA) {
        this(valueA, operatorA, null, null);
    }

}
