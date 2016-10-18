package com.dataart.ryft.elastic.parser.dsl;

public enum RyftOperator implements RyftDslToken {

    EQUALS, NOT_EQUALS, CONTAINS, NOT_CONTAINS;

    @Override
    public String buildRyftString() {
        return name();
    }

}
