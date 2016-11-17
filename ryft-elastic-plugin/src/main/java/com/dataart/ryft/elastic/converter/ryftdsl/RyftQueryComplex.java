package com.dataart.ryft.elastic.converter.ryftdsl;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

public class RyftQueryComplex implements RyftQuery {

    public static enum RyftLogicalOperator implements RyftDslToken {
        AND, OR, XOR;

        @Override
        public String buildRyftString(Boolean isIndexedSearch) {
            return name();
        }
    }

    private final Collection<RyftQuery> operands;
    private final RyftLogicalOperator operator;

    public RyftQueryComplex(RyftQuery operand1, RyftLogicalOperator operator, RyftQuery operand2) {
        this(operator, operand1, operand2);
    }

    public RyftQueryComplex(RyftLogicalOperator operator, RyftQuery... operands) {
        this(operator, Arrays.asList(operands));
    }

    public RyftQueryComplex(RyftLogicalOperator operator, Collection<RyftQuery> operands) {
        this.operands = operands;
        this.operator = operator;
    }

    @Override
    public String buildRyftString(Boolean isIndexedSearch) {
        if (operands.size() == 1) {
            return operands.iterator().next().buildRyftString(isIndexedSearch);
        } else {
            String queryString = operands.stream()
                    .map(operand -> operand.buildRyftString(isIndexedSearch))
                    .collect(Collectors.joining(" " + operator.buildRyftString(isIndexedSearch) + " "));
            return String.format("(%s)", queryString);
        }
    }

    @Override
    public String toString() {
        return "RyftQueryComplex{" + buildRyftString(false) + '}';
    }

}
