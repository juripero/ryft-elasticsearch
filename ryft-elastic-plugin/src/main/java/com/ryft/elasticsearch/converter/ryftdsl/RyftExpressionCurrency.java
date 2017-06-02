package com.ryft.elasticsearch.converter.ryftdsl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class RyftExpressionCurrency extends RyftExpressionRange {

    private final String separator;
    private final String decimal;
    private final String currency;

    public RyftExpressionCurrency(String valueA, RyftOperatorCompare operatorA, RyftOperatorCompare operatorB, String valueB, String currency, String separator, String decimal) {
        super("CURRENCY");
        if (valueA.contains(currency)) {
            valueA = valueA.replace(currency, "");
        }
        if (valueB != null && valueB.contains(currency)) {
            valueB = valueB.replace(currency, "");
        }
        this.valueA = String.format("\"%s%s\"", currency, valueA);
        this.operatorA = operatorA;
        this.variableName = "CUR";
        this.valueB = Optional.ofNullable(String.format("\"%s%s\"", currency, valueB));
        this.operatorB = Optional.ofNullable(operatorB);
        constructValue();

        this.separator = separator;
        this.decimal = decimal;
        this.currency = currency;
    }

    public RyftExpressionCurrency(String valueA, RyftOperatorCompare operatorA, String currency, String separator, String decimal) {
        this(valueA, operatorA, null, null, currency, separator, decimal);
    }

    public RyftExpressionCurrency(String valueA, RyftOperatorCompare operatorA, RyftOperatorCompare operatorB, String valueB) {
        this(valueA, operatorA, operatorB, valueB, "$", ",", ".");
    }

    public RyftExpressionCurrency(String valueA, RyftOperatorCompare operatorA) {
        this(valueA, operatorA, null, null);
    }

    @Override
    protected List<String> getParameters() {
        List<String> result = new ArrayList<>();

        result.add(String.format("\"%s\"", currency));
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
