package com.ryft.elasticsearch.converter.ryftdsl;

import java.util.Arrays;
import java.util.List;

public class RyftExpressionNumeric extends RyftExpressionRange {

    private final String separator;
    private final String decimal;

    public RyftExpressionNumeric(String valueA, RyftOperatorCompare operatorA, RyftOperatorCompare operatorB, String valueB, String separator, String decimal) {
        super(String.format("\"%s\"", valueA), operatorA, operatorB, String.format("\"%s\"", valueB), "NUMBER", "NUM");
        this.separator = separator;
        this.decimal = decimal;
    }

    public RyftExpressionNumeric(String valueA, RyftOperatorCompare operatorA, String separator, String decimal) {
        this(valueA, operatorA, null, null, separator, decimal);
    }

    public RyftExpressionNumeric(String valueA, RyftOperatorCompare operatorA, RyftOperatorCompare operatorB, String valueB) {
        this(valueA, operatorA, operatorB, valueB, ",", ".");
    }

    public RyftExpressionNumeric(String valueA, RyftOperatorCompare operatorA) {
        this(valueA, operatorA, ",", ".");
    }

    @Override
    protected List<String> getParameters() {
        return Arrays.asList(new String[]{
            String.format("\"%s\"", separator),
            String.format("\"%s\"", decimal)
        });
    }

}
