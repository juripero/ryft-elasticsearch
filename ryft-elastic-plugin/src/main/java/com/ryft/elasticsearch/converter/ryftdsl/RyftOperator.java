package com.ryft.elasticsearch.converter.ryftdsl;

public enum RyftOperator implements RyftDslToken {

    EQUALS, NOT_EQUALS, CONTAINS, NOT_CONTAINS;

    @Override
    public String buildRyftString() {
        return name();
    }

}
