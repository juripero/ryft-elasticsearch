package com.ryft.elasticsearch.converter.ryftdsl;

import java.util.ArrayList;
import java.util.List;

public class RyftExpressionIPv6 extends RyftExpressionRange {

    public RyftExpressionIPv6(String valueA, RyftOperatorCompare operatorA, RyftOperatorCompare operatorB, String valueB) {
        super(String.format("\"%s\"", valueA), operatorA, operatorB, String.format("\"%s\"", valueB), "IPV6", "IP");
    }

    public RyftExpressionIPv6(String valueA, RyftOperatorCompare operatorA) {
        this(valueA, operatorA, null, null);
    }

    @Override
    protected List<String> getParameters() {
        return new ArrayList<>();
    }

}
