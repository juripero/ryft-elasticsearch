package com.ryft.elasticsearch.converter.ryftdsl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

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
        List<String> result = new ArrayList<>();

        result.add(String.format("\"%s\"", separator));
        result.add(String.format("\"%s\"", decimal));

        if (line != null) {
            result.add(String.format("LINE=%b", line));
        }
        if (width != null) {
            result.add(String.format("WIDTH=%d", width));
        }
        return result;
    }

}
