package com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl;

import java.util.Arrays;
import java.util.List;

public class RyftExpressionNumeric extends RyftExpressionRange {

    private final String subitizer;
    private final String decimal;

    public RyftExpressionNumeric(Double valueA, RyftOperatorCompare operatorA, RyftOperatorCompare operatorB, Double valueB, String subitizer, String decimal) {
        super(String.format("\"%s\"", valueA), operatorA, operatorB, String.format("\"%s\"", valueB), "NUMBER", "NUM");
        this.subitizer = subitizer;
        this.decimal = decimal;
    }

    public RyftExpressionNumeric(Double valueA, RyftOperatorCompare operatorA, String subitizer, String decimal) {
        this(valueA, operatorA, null, null, subitizer, decimal);
    }

    public RyftExpressionNumeric(Double valueA, RyftOperatorCompare operatorA, RyftOperatorCompare operatorB, Double valueB) {
        this(valueA, operatorA, operatorB, valueB, ",", ".");
    }

    public RyftExpressionNumeric(Double valueA, RyftOperatorCompare operatorA) {
        this(valueA, operatorA, ",", ".");
    }

    @Override
    protected List<String> getParameters() {
        return Arrays.asList(new String[]{
            String.format("\"%s\"", subitizer),
            String.format("\"%s\"", decimal)
        });
    }

}
