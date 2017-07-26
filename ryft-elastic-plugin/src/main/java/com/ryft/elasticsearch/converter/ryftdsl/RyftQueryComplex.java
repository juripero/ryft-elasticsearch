package com.ryft.elasticsearch.converter.ryftdsl;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

public class RyftQueryComplex implements RyftQuery {

    public static enum RyftLogicalOperator implements RyftDslToken {
        AND, OR, XOR;

        @Override
        public String buildRyftString() {
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
    public String buildRyftString() {
        if (operands.size() == 1) {
            return operands.iterator().next().buildRyftString();
        } else {
            String queryString = operands.stream()
                    .map(operand -> operand.buildRyftString())
                    .collect(Collectors.joining(" " + operator.buildRyftString() + " "));
            return String.format("(%s)", queryString);
        }
    }

    @Override
    public RyftQuery toRawTextQuery() {
        //For raw_text search, there is an extra constraint: if the operator is AND, LINE must be true. Check and apply
        //transformations if required.
        if (operator.equals(RyftLogicalOperator.AND)) {
            return new RyftQueryComplex(operator, operands.stream().map(ryftQuery -> ryftQuery.toRawTextQuery().toLineQuery()).collect(Collectors.toList()));
        } else {
            return new RyftQueryComplex(operator, operands.stream().map(RyftQuery::toRawTextQuery).collect(Collectors.toList()));
        }
    }

    @Override
    public RyftQuery toLineQuery() {
        if (operator.equals(RyftLogicalOperator.AND)) {
            return new RyftQueryComplex(operator, operands.stream().map(RyftQuery::toLineQuery).collect(Collectors.toList()));
        }
        return this;
    }

    @Override
    public RyftQuery toWidthQuery(Integer width) {
        return new RyftQueryComplex(operator, operands.stream().map(ryftQuery -> ryftQuery.toWidthQuery(width)).collect(Collectors.toList()));
    }

    @Override
    public String toString() {
        return "RyftQueryComplex{" + buildRyftString() + '}';
    }

}
