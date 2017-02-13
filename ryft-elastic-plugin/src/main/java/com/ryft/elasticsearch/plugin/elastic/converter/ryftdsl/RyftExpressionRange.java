package com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl;

import java.util.Optional;
import javax.naming.OperationNotSupportedException;

public abstract class RyftExpressionRange extends RyftExpression {

    public static enum RyftOperatorCompare implements RyftDslToken {
        EQ("="), NE("!="), GT(">"), LT("<"), GTE(">="), LTE("<=");

        private final String ryftValue;

        private RyftOperatorCompare(String ryftValue) {
            this.ryftValue = ryftValue;
        }

        @Override
        public String buildRyftString() {
            return ryftValue;
        }

    }

    protected String valueA;
    protected RyftOperatorCompare operatorA;
    protected Optional<String> valueB = Optional.empty();
    protected Optional<RyftOperatorCompare> operatorB = Optional.empty();
    protected String variableName;

    public RyftExpressionRange(String valueA, RyftOperatorCompare operatorA, RyftOperatorCompare operatorB, String valueB, String expressionName, String variableName) {
        this(valueA, operatorA, expressionName, variableName);
        this.valueB = Optional.ofNullable(valueB);
        this.operatorB = Optional.ofNullable(operatorB);
        constructValue();
    }

    public RyftExpressionRange(String valueA, RyftOperatorCompare operatorA, String expressionName, String variableName) {
        this(expressionName);
        this.valueA = valueA;
        this.operatorA = operatorA;
        this.variableName = variableName;
        constructValue();
    }

    protected RyftExpressionRange(String expressionName) {
        super(expressionName);
    }

    protected final void constructValue() {
        if (valueB.isPresent() && operatorB.isPresent()) {
            value = String.format("%s %s %s %s %s", valueA, operatorA.buildRyftString(), variableName, operatorB.get().buildRyftString(), valueB.get());
        } else {
            value = String.format("%s %s %s", variableName, operatorA.buildRyftString(), valueA);
        }
    }

    @Override
    public RyftExpression toLineExpression() {
        //TODO:
        throw new UnsupportedOperationException("Range expression has no line option");
    }

}
