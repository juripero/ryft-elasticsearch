package com.dataart.ryft.elastic.parser.dsl;

public class RyftQueryComplex implements RyftQuery {

    public static enum RyftLogicalOperator implements RyftDslToken {
        AND, OR, XOR;

        @Override
        public String buildRyftString() {
            return name();
        }
    }

    private final RyftQuery operand1;
    private final RyftQuery operand2;
    private final RyftLogicalOperator operator;

    public RyftQueryComplex(RyftQuery operand1, RyftLogicalOperator operator, RyftQuery operand2) {
        this.operand1 = operand1;
        this.operator = operator;
        this.operand2 = operand2;
    }

    @Override
    public String buildRyftString() {
        return String.format("(%s %s %s)", operand1.buildRyftString(),
                operator.buildRyftString(), operand2.buildRyftString());
    }

}
