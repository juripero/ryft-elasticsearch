package com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl;

import java.util.Arrays;
import java.util.List;

public class RyftExpressionCurrency extends RyftExpressionRange {

    private final String subitizer;
    private final String decimal;
    private final String currency;

    public RyftExpressionCurrency(Double valueA, RyftOperatorCompare operatorA, RyftOperatorCompare operatorB, Double valueB, String currency, String subitizer, String decimal) {
        super(String.format("\"%s%s\"", currency, valueA), operatorA, operatorB, String.format("\"%s%s\"", currency, valueB), "CURRENCY", "CUR");
        this.subitizer = subitizer;
        this.decimal = decimal;
        this.currency = currency;
    }

    public RyftExpressionCurrency(Double valueA, RyftOperatorCompare operatorA, String currency, String subitizer, String decimal) {
        this(valueA, operatorA, null, null, currency, subitizer, decimal);
    }

    public RyftExpressionCurrency(Double valueA, RyftOperatorCompare operatorA, RyftOperatorCompare operatorB, Double valueB) {
        this(valueA, operatorA, operatorB, valueB, "$", ",", ".");
    }

    public RyftExpressionCurrency(Double valueA, RyftOperatorCompare operatorA) {
        this(valueA, operatorA, null, null);
    }

    @Override
    protected List<String> getParameters() {
        return Arrays.asList(new String[]{
            String.format("\"%s\"", currency),
            String.format("\"%s\"", subitizer),
            String.format("\"%s\"", decimal)
        });
    }

}
