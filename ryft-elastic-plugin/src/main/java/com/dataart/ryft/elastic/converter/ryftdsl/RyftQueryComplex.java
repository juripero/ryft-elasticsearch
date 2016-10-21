package com.dataart.ryft.elastic.converter.ryftdsl;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class RyftQueryComplex implements RyftQuery {

    public static enum RyftLogicalOperator implements RyftDslToken {
        AND, OR, XOR;

        @Override
        public String buildRyftString() {
            return name();
        }
    }

    private final List<RyftQuery> operands;
    private final RyftLogicalOperator operator;

    public RyftQueryComplex(RyftQuery operand1, RyftLogicalOperator operator, RyftQuery operand2) {
        this(operator, operand1, operand2);
    }

    public RyftQueryComplex(RyftLogicalOperator operator, RyftQuery... operands) {
        this(operator, Arrays.asList(operands));
    }
    
    public RyftQueryComplex(RyftLogicalOperator operator, List<RyftQuery> operands) {
        this.operands = operands;
        this.operator = operator;
    }

    @Override
    public String buildRyftString() {
        String queryString = operands.stream()
                .map(operand -> operand.buildRyftString())
                .collect(Collectors.joining(" " + operator.buildRyftString() + " "));
        return String.format("(%s)", queryString);
    }

}
